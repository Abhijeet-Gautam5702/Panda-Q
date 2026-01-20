/**
 * Centralized Type Definitions
 * 
 * This file contains all shared types, interfaces, and enums used across the application.
 */

// ============================================
// BROKER TYPES
// ============================================

export type PartitionId = string;
export type TopicId = string;
export type BrokerId = string;

// ============================================
// MESSAGE TYPES
// ============================================

export type Message = {
    topicId: TopicId;
    messageId: string;
    content: string;
}

// ============================================
// FILE SYSTEM TYPES
// ============================================

export type FilePath = string;

// ============================================
// VALIDATION TYPES
// ============================================

export type ValidationResult = {
    isValid: boolean;
    error?: string;
};

// ============================================
// LOG FILE TYPES
// ============================================

export enum LOG_FILE_TYPE {
    INGRESS_BUFFER,
    PARTITION_BUFFER
}
