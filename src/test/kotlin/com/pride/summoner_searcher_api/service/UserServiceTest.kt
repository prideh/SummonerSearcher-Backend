package com.pride.summoner_searcher_api.service

import com.pride.summoner_searcher_api.model.User
import com.pride.summoner_searcher_api.repository.UserRepository
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk

class UserServiceTest : BehaviorSpec({
    val userRepository = mockk<UserRepository>()
    val userService = UserService(userRepository)

    Given("A user service") {
        When("finding a user by email") {
            val email = "test@example.com"
            val user = User(email = email, hashedPassword = "password")
            every { userRepository.findByEmail(email) } returns user

            val result = userService.findByEmail(email)

            Then("it should return the user") {
                result shouldBe user
            }
        }
    }
})
