package com.pride.summoner_searcher_api.model

import jakarta.persistence.*
import java.time.Instant

@Entity
@Table(name = "refresh_token")
class RefreshToken(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,

    @Column(nullable = false, unique = true)
    var token: String,

    @Column(nullable = false)
    var expiryDate: Instant,

    @ManyToOne
    @JoinColumn(name = "user_id", referencedColumnName = "id")
    var user: User
)
