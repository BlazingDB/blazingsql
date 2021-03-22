
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BlazingColumnView.h:

Program Listing for File BlazingColumnView.h
============================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BlazingColumnView.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/BlazingColumnView.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include "execution_graph/logic_controllers/LogicPrimitives.h"
   #include "execution_graph/logic_controllers/BlazingColumn.h"
   
   namespace ral {
   
   namespace frame {
   
   class BlazingColumnView : public BlazingColumn {
       public:
           BlazingColumnView() =default;
           BlazingColumnView(const BlazingColumn&) =delete;
           BlazingColumnView& operator=(const BlazingColumnView&) =delete;
           BlazingColumnView(const CudfColumnView & column) : column(column) {};
           ~BlazingColumnView() = default;
           CudfColumnView view() const {
               return column;
           }
           // release of a BlazingColumnView will make a copy since its not the owner and therefore cannot transfer ownership
           std::unique_ptr<CudfColumn> release() { return std::make_unique<CudfColumn>(column); }
           blazing_column_type type() { return blazing_column_type::VIEW; }
           
       private:
           CudfColumnView column;
   };
   
   }  // namespace frame
   
   }  // namespace ral
