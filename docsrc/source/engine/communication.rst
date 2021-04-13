
Communication
=============

Blazing can leverage multiple communication backends and makes use of :doc:`Cache Machines <caches>` in order to isolate the graph and execution code from the code that actually performs communication. All communication is initiated by a kernel. Kernels that can communicate must be derived from a distributing_kernel. It is only through the methods available to distributing_kernel derived classes that a kernel performs communication.

The classes that must be implemented are derived from message_receiver, message_listener, message_sender, and buffer_transport. These classes contain all logic that can be shared between different interfaces



Messages
--------

All node to node communications follow the structure below.

Structure
^^^^^^^^^

.. image:: /_static/resources/comm-buffers.jpg

A message consists of both a dataframe and metadata. The dataframe can be empty in the case of sending messages that only contain plan information. The metadata is just a std::map<std::string,std::string>.


Memory Layout
^^^^^^^^^^^^^
It is not always clear, particularly when leveraging hardware such as the GPU, that the NIC will have direct access to the memory being used to operate on data. Allocations are the bain of performance. For transporting we use fixed size buffers and pack a table into 1 or more buffers.

.. image:: /_static/resources/comm-buffers.jpg

This allows both the send of receivers of messages to use fixed size pre allocated buffers for transporting information back and forth. Making sending and receiving very fast. It has the overhead of a memcpy for operations where the data on the sending side resided in a space the NIC can read from.


Classes
-------

Message Sender
^^^^^^^^^^^^^^

The purpose of the Sender is to be polling the output message cache for messages that need to be sent off to other nodes. It takes the message to be sent gi


Communication Protocols
-----------------------


TCP
^^^

UCX
^^^
