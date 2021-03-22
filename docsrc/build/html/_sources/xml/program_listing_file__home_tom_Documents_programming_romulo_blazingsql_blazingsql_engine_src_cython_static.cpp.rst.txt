
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_cython_static.cpp:

Program Listing for File static.cpp
===================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_cython_static.cpp>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/cython/static.cpp``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #include "engine/static.h"
   
   #include <vector>
   
   #include "bsqlengine_config.h"
   #include "Util/StringUtil.h"
   
   std::map<std::string, std::string> getProductDetails() {
       std::map<std::string, std::string> ret;
       std::vector<std::pair<std::string, std::string>> descriptiveMetadata = BLAZINGSQL_DESCRIPTIVE_METADATA;
       for (auto description : descriptiveMetadata) {
           ret[description.first] = StringUtil::trim(description.second);
       }
       return ret;
   }
