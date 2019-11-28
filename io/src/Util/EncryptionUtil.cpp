/*
 * EncryptionUtil.cpp
 *
 *  Created on: Feb 4, 2018
 */

#include "EncryptionUtil.h"

static std::string encryptDecrypt(const std::string &value) {
	char key = 'Q';
	std::string rotation;
	rotation.reserve(value.size());
	for (const auto chr : value) {
		rotation.push_back(chr ^ key);
	}
	return rotation;
}

namespace EncryptionUtil {

std::string encrypt(const std::string &toEncrypt) {
    return encryptDecrypt(toEncrypt);
}

std::string decrypt(const std::string &toDecrypt) {
	return encryptDecrypt(toDecrypt);
}

}
