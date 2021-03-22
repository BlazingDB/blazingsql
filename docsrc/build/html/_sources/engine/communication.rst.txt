
Communication
=============

Blazing can leverage multiple communication backends and makes use of :doc:`Cache Machines <caches>` in order to isolate the graph and execution code from the code that actually performs communication. All communication is initiated by a kernel. Kernels that can communicate must be derived from a distributing_kernel. It is only through the methods available to distributing_kernel derived classes that a kernel performs communication.

The classes that must be implemented are derived from message_receiver, message_listener, message_sender, and buffer_transport. These classes contain all logic that can be shared between different interfaces



TCP
---

UCX
---

Memory Layout
-------------

It is not always clear, particularly when leveraging hardware such as the GPU, that the NIC will have direct access to the memory being used to operate on data. Messages for this reason can be transported in various ways using either the memory in the space in which it lies or by packing those buffers into a series of pre allocated fixed size buffers for transmission. The latter basically operates as follows. Get a fixed size buffers and put as much of the table into it as possible. If that's less space than a column requires then said column is split up amongst multiple buffers, any space left over from the last buffer is space used for the next column. Similarly if a series of columns are particularly small, they might all reside within the same buffer. This makes the
