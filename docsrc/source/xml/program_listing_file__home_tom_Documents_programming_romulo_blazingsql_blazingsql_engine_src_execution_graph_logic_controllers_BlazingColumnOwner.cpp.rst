
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BlazingColumnOwner.cpp:

Program Listing for File BlazingColumnOwner.cpp
===============================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BlazingColumnOwner.cpp>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/BlazingColumnOwner.cpp``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #include "execution_graph/logic_controllers/BlazingColumnOwner.h"
   
   namespace ral {
   
   namespace frame {
   
   BlazingColumnOwner::BlazingColumnOwner(std::unique_ptr<CudfColumn> column) 
       : column(std::move(column)) {}
   
   
   }  // namespace frame
   
   }  // namespace ral
