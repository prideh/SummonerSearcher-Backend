package com.pride.summoner_searcher_api

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource

@SpringBootTest
// Provide dummy values for environment variables required by the application context
@TestPropertySource(properties = [
    "RIOT_API_KEY=dummy-key",
    "JWT_SECRET_KEY=dummy-secret-key-that-is-long-enough-for-the-algorithm",
    "2FA_SECRET_KEY=another-dummy-secret-key-for-2fa-encryption"
])
class SummonerSearcherApiApplicationTests {

	@Test
	fun contextLoads() {
	}

}
