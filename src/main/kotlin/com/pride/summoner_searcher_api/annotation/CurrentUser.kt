package com.pride.summoner_searcher_api.annotation

import org.springframework.security.core.annotation.AuthenticationPrincipal

/**
 * A custom annotation to resolve the currently authenticated User object.
 *
 * This annotation is a meta-annotation for Spring Security's @AuthenticationPrincipal.
 * It provides a convenient, type-safe way to access the logged-in user directly
 * as a method parameter in a controller, without needing to manually parse the Principal.
 *
 * Example Usage in a Controller:
 * ```
 * @GetMapping("/profile")
 * fun getProfile(@CurrentUser user: User): ResponseEntity<User> {
 *     // The 'user' parameter is automatically populated with the authenticated user.
 *     return ResponseEntity.ok(user)
 * }
 * ```
 */
@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
@AuthenticationPrincipal
annotation class CurrentUser
