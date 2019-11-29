/*
 * EncryptionUtil.h
 *
 *  Created on: Feb 4, 2018
 */

#ifndef _BZ_ENCRYPTUTIL_H_
#define _BZ_ENCRYPTUTIL__H_

#include <string>

namespace EncryptionUtil {
	std::string encrypt(const std::string &toEncrypt);
	std::string decrypt(const std::string &toDecrypt);
}

#endif /* _BZ_ENCRYPTUTIL__H_ */
