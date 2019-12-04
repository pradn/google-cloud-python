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
import logging
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

class PublishToPausedOrderingKeyException(Exception):
    """ Publish attempted to paused ordering key. To resume publishing, call
        the resumePublish method on the publisher Client object with this
        ordering key. Ordering keys are paused if an unrecoverable error
        occurred during publish of a batch for that key.
    """
    def __init__(self, ordering_key):
      self.ordering_key = ordering_key
      super(PublishToPausedOrderingKeyException, self).__init__()

class _OrderedSequencerStatus(object):
    """An enum-like class representing valid statuses for an OrderedSequencer.

    Starting state: ACCEPTING_MESSAGES
    Valid transitions:
      ACCEPTING_MESSAGES -> PAUSED (on permanent error)
      ACCEPTING_MESSAGES -> STOPPED  (when user stops client and when all batch
                                      publishes finish)
      PAUSED -> ACCEPTING_MESSAGES  (when user unpauses)
      PAUSED -> STOPPED  (when user stops client)
    """

    # Accepting publishes and/or waiting for result of batch publish
    ACCEPTING_MESSAGES = "accepting messages"
    # Permanent error occurred. User must unpause this sequencer to resume
    # publishing. This is done to maintain ordering.
    PAUSED = "paused"
    # Permanent failure. No more publishes allowed.
    STOPPED = "stopped"

class OrderedSequencer(object):
    """ Sequences messages into batches ordered by an ordering key for one topic.
        A sequencer always has at least one batch in it, unless paused or stopped.
        When no batches remain, the |publishes_done_callback| is called so the
        client can perform cleanup.

        Args:
            client (~.pubsub_v1.PublisherClient): The publisher client used to
                create this sequencer.
            topic (str): The topic. The format for this is
                ``projects/{project}/topics/{topic}``.
            ordering_key (str): The ordering key for this sequencer.
            publishes_done_callback (function): Callback called when this
                sequencer is done publishing all messages. This callback allows
                the client to remove sequencer state, preventing a memory leak.
                It is not called on pause, but may be called after stop().
    """
    def __init__(self, client, topic, ordering_key, publishes_done_callback):
        self._client = client
        self._topic = topic
        self._ordering_key = ordering_key
        # Guards the variables below
        self._state_lock = threading.Lock()
        self._publishes_done_callback = publishes_done_callback
        # Batches ordered from first (index 0) to last.
        # Invariant: always has at least one batch after the first publish,
        # unless paused or stopped.
        self._ordered_batches = []
        # See _OrderedSequencerStatus for valid state transitions.
        self._state = _OrderedSequencerStatus.ACCEPTING_MESSAGES

    def stop(self):
        """ Permanently stop this sequencer. This differs from pausing, which
            may be resumed. Immediately commits the first batch and cancels the
            rest.

            Raises:
                RuntimeError:
                    If called after stop() has already been called.
        """
        assert self._state == _OrderedSequencerStatus.ACCEPTING_MESSAGES or \
               self._state == _OrderedSequencerStatus.PAUSED

        with self._state_lock:
            if self._state == _OrderedSequencerStatus.STOPPED:
                raise RuntimeError("Ordered sequencer already stopped.")

            self._state = _OrderedSequencerStatus.STOPPED
            if self._ordered_batches:
                self._ordered_batches[0].commit()
                if len(self._ordered_batches) > 1:
                    for batch in self._ordered_batches[1:]:
                        batch.cancel()
                    del self._ordered_batches[1:]

    def commit(self):
        """ Commit the first batch, if unpaused. If paused or no batches
            exist, this method does nothing.

            Raises:
                RuntimeError:
                    If called after stop() has already been called.
        """
        with self._state_lock:
            if self._state == _OrderedSequencerStatus.STOPPED:
                raise RuntimeError("Ordered sequencer already stopped.")

            if self._state != _OrderedSequencerStatus.PAUSED and \
               self._ordered_batches:
                # It's okay to commit the same batch more than once. The
                # operation is idempotent.
                self._ordered_batches[0].commit()

    def _batch_done_callback(self, success):
        """ Called when a batch has finished publishing, with either a success
            or a failure. (Temporary failures are retried infinitely when
            ordering keys are enabled.)
        """
        assert self._state != _OrderedSequencerStatus.PAUSED

        with self._state_lock:
            # Message futures for the batch have been completed (either with a
            # result or an exception) already, so remove the batch.
            self._ordered_batches.pop(0)

            if success:
                if len(self._ordered_batches) == 0:
                    self._publishes_done_callback(topic, ordering_key)
                    self._state = _OrderedSequencerStatus.ACCEPTING_MESSAGES
                elif len(self._ordered_batches) > 1:
                  # If there is more than one batch, we know that the next batch
                  # must be full and, therefore, ready to be committed.
                  self._ordered_batches[0].commit()
                # if len == 1: wait for messages and/or commit timeout
            else:
                # Unrecoverable error detected
                self._state = _OrderedSequencerStatus.PAUSED
                for batch in self._ordered_batches:
                    batch.cancel()
                del self._ordered_batches[:]

    def unpause(self):
        """ Unpauses this sequencer.

        Raises:
            RuntimeError:
                If called when the ordering key has not been paused.
        """
        with self._state_lock:
            if self._state != _OrderedSequencerStatus.PAUSED:
                raise RuntimeError("Ordering key is not paused.")
            self._state = _OrderedSequencerStatus.ACCEPTING_MESSAGES

    def _create_batch(self):
        """ Creates a new batch using the client's batch class and other stored
            settings.
        """
        return self._client._batch_class(
            client=self._client,
            topic=self._topic,
            settings=self._client.batch_settings,
            batch_done_callback=_batch_done_callback,
            commit_when_full=False
        )

    def publish(self, message):
        """ Publish message for this ordering key.

        Returns:
            A class instance that conforms to Python Standard library's
            :class:`~concurrent.futures.Future` interface (but not an
            instance of that class). The future might return immediately with a
            PublishToPausedOrderingKeyException if the ordering key is paused.
            Otherwise, the future tracks the lifetime of the message publish.

        Raises:
            RuntimeError:
                If called after this sequencer has been stopped, either by
                a call to stop() or after all batches have been published.
        """
        with self._state_lock:
            if self._state == _OrderedSequencerStatus.PAUSED:
                future = futures.Future()
                exception = \
                    PublishToPausedOrderingKeyException(self._ordering_key)
                future.set_exception(exception)
                return future

            if self._state == _OrderedSequencerStatus.STOPPED:
                raise RuntimeError("Cannot publish on a stopped sequencer.")

            assert self._state == _OrderedSequencerStatus.ACCEPTING_MESSAGES

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

