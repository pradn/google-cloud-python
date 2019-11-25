# Copyright 2019, Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

import concurrent.futures as futures
import copy
import os
import pkg_resources
import threading
import time

import grpc
import six

from google.api_core import grpc_helpers
from google.oauth2 import service_account

from google.cloud.pubsub_v1 import _gapic
from google.cloud.pubsub_v1 import types
from google.cloud.pubsub_v1.gapic import publisher_client
from google.cloud.pubsub_v1.gapic.transports import publisher_grpc_transport
from google.cloud.pubsub_v1.publisher._batch import thread


__version__ = pkg_resources.get_distribution("google-cloud-pubsub").version

_BLACKLISTED_METHODS = (
    "publish",
    "from_service_account_file",
    "from_service_account_json",
)

def _set_nested_value(container, value, keys):
    current = container
    for key in keys[:-1]:
        if current.get(key) is None:
          current[key] = {}
        current = current[key]
    current[keys[-1]] = value
    return container

class PublishToFrozenOrderingKeyException(Exception):
    """ Publish attempted to frozen ordering key. To resume publishing, call
        the resumePublish method on the publisher Client object. Ordering keys
        are frozen if an unrecoverable error occurred during publish a batch
        for that key.
    """
    def __init__(self, ordering_key):
      self.ordering_key = ordering_key
      super(PublishToFrozenOrderingKeyException, self).__init__()

# TODO: use edge-triggering
class _PeriodicCommitter(object):
    def __init__(self, commit_period):
        self._lock = threading.Lock()
        self._sequencers = []
        self._commit_period = commit_period

        self._thread = threading.Thread(
              name="Thread-PubSubPublisherPeriodicCommitter",
              target=self._commit
          )
        self._thread.start()

    def add_sequencer(sequencer):
        with self._lock:
            self._sequencers.append(sequencer)

    def _commit(self):
        # todo figure out how to stop
        #while True:
        for i in range(0, 10):
            time.sleep(self._commit_period)

            for sequencer in self._sequencers:
                with self._lock:
                    sequencer.commit()

"""
TODO pradn
1. periodic commit
2. state transitions simplify
3. tests
"""

class _OrderedSequencerStatus(object):
    """An enum-like class representing valid statuses for an OrderedSequencer.

    Starting state: ACCEPTING_MESSAGES
    Valid transitions:
      ACCEPTING_MESSAGES -> PAUSED (on permanent error)
      ACCEPTING_MESSAGES -> STOPPED  (when user stops client)
      PAUSED -> ACCEPTING_MESSAGES  (when user unpauses)
      PAUSED -> STOPPED  (when user stops client)
    """

    # Accepting publishes and/or waiting for result of batch publish
    ACCEPTING_MESSAGES = "accepting messages"
    # Permanent error occurred. User must unpause this sequencer to resume
    # publishing.
    PAUSED = "paused"
    # Permanent failure. No more publishes allowed.
    STOPPED = "stopped"

class _OrderedSequencer(object):
    """ Sequences messages into batches ordered by an ordering key for one topic.
        A sequencer always has at least one batch in it. When no batches remain,
        the |publishes_done_callback| is called so the client can perform cleanup.

        Args:
            client (~.pubsub_v1.PublisherClient): The publisher client used to
                create this sequencer.
            topic (str): The topic. The format for this is
                ``projects/{project}/topics/{topic}``.
            ordering_key (str): The ordering key for this sequencer.
            publishes_done_callback (function): Callback called when this
                sequencer is done publishing all messages. This callback allows
                the client to remove sequencer state, preventing a memory leak.
                It is not called on stop() or when paused.
    """
    def __init__(self, client, topic, ordering_key, publishes_done_callback):
        self._client = client
        self._topic = topic
        self._ordering_key = ordering_key
        # Guards the variables below
        self._state_lock = threading.Lock()
        self._publishes_done_callback = publishes_done_callback
        # Batches ordered from first (index 0) to last.
        # Invariant: always has at least one batch after the first publish.
        self._ordered_batches = []
        # Permanent errors during publish result in this sequencer being paused.
        # This is done to maintain ordering. The user must unpause the
        # sequencer to resume publishing.
        self._paused = False
        # Permanently stopped. Unlike pause, you can't go back from this phase.
        self._stopped = False

    def stop(self):
        """ Permanently stop this sequencer. This differs from pausing, which
            may be resumed. Immediately commits the first batch and cancels the
            rest.
        """
        with self._state_lock:
            self._stopped = True
            if self._ordered_batches:
                self._ordered_batches[0].commit()
                if len(self._ordered_batches) > 1:
                    for batch in self._ordered_batches[1:]:
                        batch.cancel()
                    del self._ordered_batches[1:]

    def commit(self):
        """ Commit the first batch, if unpaused.
        """
        with self._state_lock:
            if not self._paused and self._ordered_batches:
                # It's okay to commit the same batch more than once. The operation
                # is idempotent.
                self._ordered_batches[0].commit()

    def _batch_done_callback(self, success):
        with self._state_lock:
            # Message futures for the batch have been completed (either with a
            # result or an exception) already, so remove the batch.
            self._ordered_batches.pop(0)

            if success:
                if len(self._ordered_batches) == 0:
                    self._publishes_done_callback(topic, ordering_key)
                    self._stopped = True
                elif len(self._ordered_batches) > 1:
                  # If there is more than one batch, we know that the next batch
                  # must be full and, therefore, ready to be committed.
                  self._ordered_batches[0].commit()
                # if len == 1: wait for messages and/or commit timeout
            # Unrecoverable error
            else:
                self._paused = True
                for batch in self._ordered_batches:
                    batch.cancel()
                del self._ordered_batches[:]

    def unpause(self):
        with self._state_lock:
            if not self._paused:
                raise RuntimeError("Ordering key is not paused.")
            self._paused = False

    def _create_batch(self):
        return self._client._batch_class(
            client=self._client,
            topic=self._topic,
            settings=self._client.batch_settings,
            batch_done_callback=_batch_done_callback,
            autocommit=False,
            commit_when_full=False
        )

    def publish(self, message):
        with self._state_lock:
            if self._paused:
                future = futures.Future()
                exception = PublishToFrozenOrderingKeyException(self._ordering_key)
                future.set_exception(exception)
                return future

            if self._stopped:
                return None

            if not self._ordered_batches:
                new_batch = self._create_batch()
                self._ordered_batches.append(new_batch)

            batch = self._ordered_batches[-1]
            future = batch.publish(message)
            while future is None:
                batch = self._create_batch()
                self._ordered_batches.append(batch)
                future = batch.publish(message)

            return future

    # Used only for testing.
    def _set_batch(self, batch):
        self._ordered_batches = [batch]


# Not thread-safe.
class _UnorderedSequencer(object):
    """ Sequences messages into batches for one topic without any ordering.
    """
    def __init__(self, client, topic):
        self._client = client
        self._topic = topic
        self._current_batch = None

    def stop(self):
        self.commit()

    def commit(self):
        if self._current_batch:
            self._current_batch.commit()

    def _get_or_create_batch(self, create=False):
        """ This will create a new batch if ``create=True`` or if no batch
        currently exists.

        Args:
            create (bool): Whether to create a new batch. Defaults to
                :data:`False`. If :data:`True`, this will create a new batch
                even if one already exists.
            autocommit (bool): Whether to autocommit this batch. This is
                primarily useful for debugging and testing, since it allows
                the caller to avoid some side effects that batch creation
                might have (e.g. spawning a worker to publish a batch).

        Returns:
            ~.pubsub_v1._batch.Batch: The batch object.
        """
        if not create:
            batch = self._current_batch
            if batch is None:
                create = True

        if create:
            batch = self._client._batch_class(
                client=self._client,
                topic=self._topic,
                settings=self._client.batch_settings,
                batch_done_callback=None,
                autocommit=True,
                commit_when_full=True
            )
            self._current_batch = batch

        return batch

    def publish(self, message):
        """ Batch message into existing or new batch.

        Args:
            message (~.pubsub_v1.types.PubsubMessage): The Pub/Sub message.

        Returns:
            ~google.api_core.future.Future: An object conforming to
            the :class:`~concurrent.futures.Future` interface. The future tracks
            the publishing status of the message.
        """
        batch = self._get_or_create_batch()
        future = None
        while future is None:
          future = batch.publish(message)
          if future is None:
              batch = self._get_or_create_batch(create=True)
        return future

    # Used only for testing.
    def _set_batch(self, batch):
        self._current_batch = batch


@_gapic.add_methods(publisher_client.PublisherClient, blacklist=_BLACKLISTED_METHODS)
class Client(object):
    """A publisher client for Google Cloud Pub/Sub.

    This creates an object that is capable of publishing messages.
    Generally, you can instantiate this client with no arguments, and you
    get sensible defaults.

    Args:
        publisher_options (~google.cloud.pubsub_v1.types.PublisherOptions): The
            options for the publisher client. Note that enabling message ordering will
            override the publish retry timeout to be infinite.
        batch_settings (~google.cloud.pubsub_v1.types.BatchSettings): The
            settings for batch publishing.
        kwargs (dict): Any additional arguments provided are sent as keyword
            arguments to the underlying
            :class:`~google.cloud.pubsub_v1.gapic.publisher_client.PublisherClient`.
            Generally you should not need to set additional keyword
            arguments. Optionally, publish retry settings can be set via
            ``client_config`` where user-provided retry configurations are
            applied to default retry settings. And regional endpoints can be
            set via ``client_options`` that takes a single key-value pair that
            defines the endpoint.

    Example:

    .. code-block:: python

        from google.cloud import pubsub_v1

        publisher_client = pubsub_v1.PublisherClient(
            # Optional
            publisher_options = pubsub_v1.types.PublisherOptions(
                enable_message_ordering=False
            ),

            # Optional
            batch_settings = pubsub_v1.types.BatchSettings(
                max_bytes=1024,  # One kilobyte
                max_latency=1,   # One second
            ),

            # Optional
            client_config = {
                "interfaces": {
                    "google.pubsub.v1.Publisher": {
                        "retry_params": {
                            "messaging": {
                                'total_timeout_millis': 650000,  # default: 600000
                            }
                        }
                    }
                }
            },

            # Optional
            client_options = {
                "api_endpoint": REGIONAL_ENDPOINT
            }
        )
    """

    def __init__(self, publisher_options=(), batch_settings=(), **kwargs):
        # Sanity check: Is our goal to use the emulator?
        # If so, create a grpc insecure channel with the emulator host
        # as the target.
        if os.environ.get("PUBSUB_EMULATOR_HOST"):
            kwargs["channel"] = grpc.insecure_channel(
                target=os.environ.get("PUBSUB_EMULATOR_HOST")
            )

        # Use a custom channel.
        # We need this in order to set appropriate default message size and
        # keepalive options.
        if "transport" not in kwargs:
            channel = kwargs.pop("channel", None)
            if channel is None:
                channel = grpc_helpers.create_channel(
                    credentials=kwargs.pop("credentials", None),
                    target=self.target,
                    scopes=publisher_client.PublisherClient._DEFAULT_SCOPES,
                    options={
                        "grpc.max_send_message_length": -1,
                        "grpc.max_receive_message_length": -1,
                    }.items(),
                )
            # cannot pass both 'channel' and 'credentials'
            kwargs.pop("credentials", None)
            transport = publisher_grpc_transport.PublisherGrpcTransport(channel=channel)
            kwargs["transport"] = transport

        # For a transient failure, retry publishing the message infinitely.
        self.publisher_options = types.PublisherOptions(*publisher_options)
        self._enable_message_ordering = self.publisher_options[0]
        if self._enable_message_ordering:
            # Set retry timeout to "infinite" when message ordering is enabled.
            # Note that this then also impacts messages added with an empty ordering
            # key.
            client_config = _set_nested_value(
                kwargs.pop("client_config", {}), 2**32,
                ["interfaces", "google.pubsub.v1.Publisher", "retry_params",
                "messaging", "total_timeout_millis"])
            kwargs["client_config"] = client_config

        # Add the metrics headers, and instantiate the underlying GAPIC
        # client.
        self.api = publisher_client.PublisherClient(**kwargs)
        self._batch_class = thread.Batch
        self.batch_settings = types.BatchSettings(*batch_settings)

        # The batches on the publisher client are responsible for holding
        # messages. One batch exists for each topic.
        self._batch_lock = self._batch_class.make_lock()
        # (topic, ordering_key) => sequencers object
        self._sequencers = {}
        #self._periodic_committer = _PeriodicCommitter(self.batch_settings.max_latency)
        self._is_stopped = False

    @classmethod
    def from_service_account_file(cls, filename, batch_settings=(), **kwargs):
        """Creates an instance of this client using the provided credentials
        file.

        Args:
            filename (str): The path to the service account private key json

                file.
            batch_settings (~google.cloud.pubsub_v1.types.BatchSettings): The
                settings for batch publishing.
            kwargs: Additional arguments to pass to the constructor.

        Returns:
            A Publisher :class:`~google.cloud.pubsub_v1.publisher.client.Client`
            instance that is the constructed client.
        """
        credentials = service_account.Credentials.from_service_account_file(filename)
        kwargs["credentials"] = credentials
        return cls(batch_settings, **kwargs)

    from_service_account_json = from_service_account_file

    @property
    def target(self):
        """Return the target (where the API is).

        Returns:
            str: The location of the API.
        """
        return publisher_client.PublisherClient.SERVICE_ADDRESS

    def _delete_sequencer(self, topic, ordering_key):
        with self._batch_lock:
            sequencer_key = (topic, ordering_key)
            del self._sequencers[sequencer_key]

    def _get_or_create_sequencer(self, topic, ordering_key):
        sequencer_key = (topic, ordering_key)
        sequencer = self._sequencers.get(sequencer_key)
        if sequencer is None:
            if ordering_key == "":
              sequencer = _UnorderedSequencer(self, topic)
            else:
              sequencer = _OrderedSequencer(self, topic, ordering_key, _delete_sequencer)
              #if self.batch_settings.max_latency < float("inf"):
              #    self._periodic_committer.add_sequencer(sequencer)
            self._sequencers[sequencer_key] = sequencer

        return sequencer

    def resume_publish(self, topic, ordering_key):
        """ Resume publish on an ordering key that has had unrecoverable errors.

        Args:
            topic (str): The topic to publish messages to.
            ordering_key: A string that identifies related messages for which
                publish order should be respected.

        Raises:
            RuntimeError:
                If called after publisher has been stopped by a `stop()` method
                call.
            ValueError:
                If the topic/ordering key combination has not been seen before
                by this client.
        """
        with self._batch_lock:
            if self._is_stopped:
                raise RuntimeError("Cannot resume publish on a stopped publisher.")

            sequencer_key = (topic, ordering_key)
            sequencer = self._sequencers.get(sequencer_key)
            if sequencer is None:
                raise ValueError(
                    "The topic/ordering key combination has not been seen before."
                )
            sequencer.unpause()

    def publish(self, topic, data, ordering_key="", **attrs):
        """Publish a single message.

        .. note::
            Messages in Pub/Sub are blobs of bytes. They are *binary* data,
            not text. You must send data as a bytestring
            (``bytes`` in Python 3; ``str`` in Python 2), and this library
            will raise an exception if you send a text string.

            The reason that this is so important (and why we do not try to
            coerce for you) is because Pub/Sub is also platform independent
            and there is no way to know how to decode messages properly on
            the other side; therefore, encoding and decoding is a required
            exercise for the developer.

        Add the given message to this object; this will cause it to be
        published once the batch either has enough messages or a sufficient
        period of time has elapsed.

        Example:
            >>> from google.cloud import pubsub_v1
            >>> client = pubsub_v1.PublisherClient()
            >>> topic = client.topic_path('[PROJECT]', '[TOPIC]')
            >>> data = b'The rain in Wales falls mainly on the snails.'
            >>> response = client.publish(topic, data, username='guido')

        Args:
            topic (str): The topic to publish messages to.
            data (bytes): A bytestring representing the message body. This
                must be a bytestring.
            ordering_key: A string that identifies related messages for which
                publish order should be respected. Message ordering must be
                enabled for this client to use this feature.
                EXPERIMENTAL: This feature is currently available in a closed
                alpha. Please contact the Cloud Pub/Sub team to use it.
            attrs (Mapping[str, str]): A dictionary of attributes to be
                sent as metadata. (These may be text strings or byte strings.)

        Returns:
            A :class:`~google.cloud.pubsub_v1.publisher.futures.Future`
            instance that conforms to Python Standard library's
            :class:`~concurrent.futures.Future` interface (but not an
            instance of that class).

        Raises:
            RuntimeError:
                If called after publisher has been stopped
                by a `stop()` method call.
        """
        # Sanity check: Is the data being sent as a bytestring?
        # If it is literally anything else, complain loudly about it.
        if not isinstance(data, six.binary_type):
            raise TypeError(
                "Data being published to Pub/Sub must be sent as a bytestring."
            )

        if (not self._enable_message_ordering and ordering_key != ""):
            raise ValueError(
                "Cannot publish a message with an ordering key when message "
                "ordering is not enabled."
            )


        # Coerce all attributes to text strings.
        for k, v in copy.copy(attrs).items():
            if isinstance(v, six.text_type):
                continue
            if isinstance(v, six.binary_type):
                attrs[k] = v.decode("utf-8")
                continue
            raise TypeError(
                "All attributes being published to Pub/Sub must "
                "be sent as text strings."
            )

        # Create the Pub/Sub message object.
        message = types.PubsubMessage(data=data, attributes=attrs,
                                      ordering_key=ordering_key)

        with self._batch_lock:
            if self._is_stopped:
                raise RuntimeError("Cannot publish on a stopped publisher.")

            sequencer = self._get_or_create_sequencer(topic, ordering_key)

            # Delegate the publishing to the sequencer.
            return sequencer.publish(message)

    # Used only for testing.
    def _set_batch(self, topic, batch, ordering_key=""):
        sequencer = self._get_or_create_sequencer(topic, ordering_key)
        sequencer._set_batch(batch)

    # Used only for testing.
    def _set_batch_class(self, batch_class):
        self._batch_class = batch_class

    def stop(self):
        """Immediately publish all outstanding messages.

        Asynchronously sends all outstanding messages and
        prevents future calls to `publish()`. Method should
        be invoked prior to deleting this `Client()` object
        in order to ensure that no pending messages are lost.

        .. note::

            This method is non-blocking. Use `Future()` objects
            returned by `publish()` to make sure all publish
            requests completed, either in success or error.
        """
        with self._batch_lock:
            if self._is_stopped:
                raise RuntimeError("Cannot stop a publisher already stopped.")

            self._is_stopped = True

            for sequencer in self._sequencers.values():
                sequencer.stop()
