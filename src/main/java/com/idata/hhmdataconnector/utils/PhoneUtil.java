package com.idata.hhmdataconnector.utils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class PhoneUtil {
    public static String encryptPhoneNumber(String phoneNumber) {
        byte[] bytes = phoneNumber.getBytes(StandardCharsets.UTF_8);
        byte[] encryptedBytes = Base64.getEncoder().encode(bytes);
        return new String(encryptedBytes, StandardCharsets.UTF_8);
    }

    // 解密电话号码
    public static String decryptPhoneNumber(String encryptedPhoneNumber) {
        byte[] encryptedBytes = encryptedPhoneNumber.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = Base64.getDecoder().decode(encryptedBytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
