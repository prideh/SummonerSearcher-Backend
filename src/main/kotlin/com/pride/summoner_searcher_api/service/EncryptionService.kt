package com.pride.summoner_searcher_api.service

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

private const val algorithm = "AES/GCM/NoPadding"
private const val ivLength = 12 // 96 bits is recommended for GCM
private const val tagLength = 128

@Service
class EncryptionService(
    // This key will be loaded from your application properties
    @Value("\${two-factor.encryption.key}") private val key: String
) {

    private val secretKeySpec: SecretKeySpec by lazy {
        // Ensure the key is the correct size (32 bytes for AES-256)
        val keyBytes = key.toByteArray(StandardCharsets.UTF_8)
        val sizedKey = ByteArray(32)
        System.arraycopy(keyBytes, 0, sizedKey, 0, minOf(keyBytes.size, sizedKey.size))
        SecretKeySpec(sizedKey, "AES")
    }

    fun encrypt(data: String): String {
        val iv = ByteArray(ivLength)
        SecureRandom().nextBytes(iv) // GCM requires a unique IV for each encryption

        val cipher = Cipher.getInstance(algorithm)
        val parameterSpec = GCMParameterSpec(tagLength, iv)
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, parameterSpec)

        val cipherText = cipher.doFinal(data.toByteArray(StandardCharsets.UTF_8))

        // Prepend IV to the ciphertext for storage, as it's needed for decryption
        val encryptedDataWithIv = iv + cipherText

        return Base64.getEncoder().encodeToString(encryptedDataWithIv)
    }

    fun decrypt(encryptedData: String): String {
        val decodedData = Base64.getDecoder().decode(encryptedData)

        // Extract the IV from the beginning of the data
        val iv = decodedData.sliceArray(0 until ivLength)
        val cipherText = decodedData.sliceArray(ivLength until decodedData.size)

        val cipher = Cipher.getInstance(algorithm)
        val parameterSpec = GCMParameterSpec(tagLength, iv)
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, parameterSpec)

        val decryptedText = cipher.doFinal(cipherText)

        return String(decryptedText, StandardCharsets.UTF_8)
    }
}
