package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.LocalDateTime

@Entity
@Table(name = "users")
data class User(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,

    @Column(unique = true, nullable = false)
    var email: String,

    @Column(nullable = false)
    var password: String,

    @Column(nullable = false)
    var darkmodePreference: Boolean = false,

    @Column(nullable = false)
    var verified: Boolean = false,

    @Column(unique = true)
    var verificationToken: String? = null,

    var verificationTokenExpiry: LocalDateTime? = null,

    @Column(unique = true)
    var passwordResetToken: String? = null,

    var passwordResetTokenExpiry: LocalDateTime? = null,

    @Column(nullable = false)
    var twoFactorEnabled: Boolean = false,

    var twoFactorSecret: String? = null,

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "user_recent_searches", joinColumns = [JoinColumn(name = "user_id")])
    @Column(name = "search_query")
    var recentSearches: MutableList<String> = mutableListOf()
)
