#!/bin/bash

# NOTE run this script as root from myconf dir (this current dir)

echo "Replacing your /etc/krb5.conf"
echo "cp krb5.conf /etc"
cp krb5.conf /etc
echo "Done"
