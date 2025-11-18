package com.pride.summoner_searcher_api.service

import com.google.zxing.BarcodeFormat
import com.google.zxing.client.j2se.MatrixToImageWriter
import com.google.zxing.qrcode.QRCodeWriter
import com.warrenstrange.googleauth.GoogleAuthenticator
import com.warrenstrange.googleauth.GoogleAuthenticatorKey
import com.warrenstrange.googleauth.GoogleAuthenticatorQRGenerator
import org.springframework.stereotype.Service
import java.io.ByteArrayOutputStream
import java.util.Base64
import javax.imageio.ImageIO

@Service
class TwoFactorAuthService(
    private val googleAuthenticator: GoogleAuthenticator
) {

    fun generateNewSecret(): String {
        return googleAuthenticator.createCredentials().key
    }

    fun createQrCodeDataUri(secret: String, email: String, issuer: String): String {
        val key = GoogleAuthenticatorKey.Builder(secret).build()
        val otpAuthUrl = GoogleAuthenticatorQRGenerator.getOtpAuthTotpURL(issuer, email, key)

        val qrCodeWriter = QRCodeWriter()
        val bitMatrix = qrCodeWriter.encode(otpAuthUrl, BarcodeFormat.QR_CODE, 250, 250)

        val pngOutputStream = ByteArrayOutputStream()
        ImageIO.write(MatrixToImageWriter.toBufferedImage(bitMatrix), "PNG", pngOutputStream)
        val pngData = pngOutputStream.toByteArray()

        return "data:image/png;base64,${Base64.getEncoder().encodeToString(pngData)}"
    }

    fun isCodeValid(secret: String, code: Int): Boolean {
        return googleAuthenticator.authorize(secret, code)
    }
}
