#ifndef NVTX_UTILS_H
#define NVTX_UTILS_H

#include <array>
#include <cassert>
#include <cstddef>  // size_t
#include <cudf/types.h>
#include <string>

#ifdef USE_NVTX
#include "nvToolsExt.h"
#endif

// TODO: When we switch to a C++ Python interface, switch to using an
// enum class instead of an enum that indexes into an array like this
std::array<const uint32_t, GDF_NUM_COLORS> const colors = {
	0xff00ff00, 0xff0000ff, 0xffffff00, 0xffff00ff, 0xff00ffff, 0xffff0000, 0xffffffff, 0xff006600, 0xffffa500};

const gdf_color JOIN_COLOR = GDF_CYAN;
const gdf_color GROUPBY_COLOR = GDF_GREEN;
const gdf_color BINARY_OP_COLOR = GDF_YELLOW;
const gdf_color PARTITION_COLOR = GDF_PURPLE;
const gdf_color READ_CSV_COLOR = GDF_PURPLE;

inline void PUSH_RANGE(std::string const & name, const gdf_color color) {
#ifdef USE_NVTX
	assert(color < GDF_NUM_COLORS);
	nvtxEventAttributes_t eventAttrib = {0};
	eventAttrib.version = NVTX_VERSION;
	eventAttrib.size = NVTX_EVENT_ATTRIB_STRUCT_SIZE;
	eventAttrib.colorType = NVTX_COLOR_ARGB;
	eventAttrib.color = colors[color];
	eventAttrib.messageType = NVTX_MESSAGE_TYPE_ASCII;
	eventAttrib.message.ascii = name.c_str();
	nvtxRangePushEx(&eventAttrib);
#endif
}

inline void PUSH_RANGE(std::string const & name, const uint32_t color) {
#ifdef USE_NVTX
	nvtxEventAttributes_t eventAttrib = {0};
	eventAttrib.version = NVTX_VERSION;
	eventAttrib.size = NVTX_EVENT_ATTRIB_STRUCT_SIZE;
	eventAttrib.colorType = NVTX_COLOR_ARGB;
	eventAttrib.color = color;
	eventAttrib.messageType = NVTX_MESSAGE_TYPE_ASCII;
	eventAttrib.message.ascii = name.c_str();
	nvtxRangePushEx(&eventAttrib);
#endif
}


inline void POP_RANGE(void) {
#ifdef USE_NVTX
	nvtxRangePop();
#endif
}


/* --------------------------------------------------------------------------*/
/**
 * @Synopsis  Start an NVTX range.
 *
 * This function is useful only for profiling with nvvp or Nsight Systems. It
 * demarcates the begining of a user-defined range with a specified name and
 * color that will show up in the timeline view of nvvp/Nsight Systems. Can be
 * nested within other ranges.
 *
 * @Param name The name of the NVTX range
 * @Param color The color to use for the range
 *
 * @Returns
 */
/* ----------------------------------------------------------------------------*/
gdf_error gdf_nvtx_range_push(char const * const name, gdf_color color) {
	if((color < 0) || (color > GDF_NUM_COLORS))
		return GDF_UNDEFINED_NVTX_COLOR;

	if(nullptr == name)
		return GDF_NULL_NVTX_NAME;

	PUSH_RANGE(name, color);

	return GDF_SUCCESS;
}

/* --------------------------------------------------------------------------*/
/**
 * @Synopsis  Start a NVTX range with a custom ARGB color code.
 *
 * This function is useful only for profiling with nvvp or Nsight Systems. It
 * demarcates the begining of a user-defined range with a specified name and
 * color that will show up in the timeline view of nvvp/Nsight Systems. Can be
 * nested within other ranges.
 *
 * @Param name The name of the NVTX range
 * @Param color The ARGB hex color code to use to color this range (e.g., 0xFF00FF00)
 *
 * @Returns
 */
/* ----------------------------------------------------------------------------*/
gdf_error gdf_nvtx_range_push_hex(char const * const name, unsigned int color) {
	if(nullptr == name)
		return GDF_NULL_NVTX_NAME;

	PUSH_RANGE(name, color);

	return GDF_SUCCESS;
}

/* --------------------------------------------------------------------------*/
/**
 * @Synopsis Ends the inner-most NVTX range.
 *
 * This function is useful only for profiling with nvvp or Nsight Systems. It
 * will demarcate the end of the inner-most range, i.e., the most recent call to
 * gdf_nvtx_range_push.
 *
 * @Returns
 */
/* ----------------------------------------------------------------------------*/
gdf_error gdf_nvtx_range_pop() {
	POP_RANGE();
	return GDF_SUCCESS;
}

#endif
