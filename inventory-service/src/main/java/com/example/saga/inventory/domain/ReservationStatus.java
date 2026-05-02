package com.example.saga.inventory.domain;

/** State of an inventory reservation. */
public enum ReservationStatus {
    /** Stock has been reserved and is being held for the order. */
    RESERVED,
    /** Reservation was released (saga compensation — order cancelled). */
    RELEASED
}
