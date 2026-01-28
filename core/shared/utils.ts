import fs from "fs";
import path from "path";
import ERROR_CODES from "./error-codes.js";
import { FilePath, ValidationResult } from "./types.js";

/**
 * Ensures a directory exists at the given path.
 * Creates the directory if it doesn't exist.
 * @param dirPath - Path to the directory
 * @returns Validation result
 */
export function ensureDirectoryExists(dirPath: string): ValidationResult {
    try {
        if (!dirPath) {
            return { isValid: false, error: ERROR_CODES.INVALID_DIRECTORY_PATH };
        }

        if (!fs.existsSync(dirPath)) {
            fs.mkdirSync(dirPath, { recursive: true });
        }

        // Verify it's actually a directory
        const stats = fs.statSync(dirPath);
        if (!stats.isDirectory()) {
            return { isValid: false, error: ERROR_CODES.INVALID_DIRECTORY_PATH };
        }

        return { isValid: true };
    } catch (error) {
        return { isValid: false, error: ERROR_CODES.DIRECTORY_NOT_FOUND };
    }
}

/**
 * Ensures a file exists at the given path.
 * Creates an empty file if it doesn't exist.
 * @param filePath - Path to the file
 * @returns Validation result
 */
export function ensureFileExists(filePath: FilePath): ValidationResult {
    try {
        if (!filePath) {
            return { isValid: false, error: ERROR_CODES.INVALID_FILE_PATH };
        }

        // Ensure parent directory exists
        const dirPath = path.dirname(filePath);
        const dirValidation = ensureDirectoryExists(dirPath);
        if (!dirValidation.isValid) {
            return dirValidation;
        }

        // Create file if it doesn't exist
        if (!fs.existsSync(filePath)) {
            fs.writeFileSync(filePath, "", "utf-8");
        }

        // Verify it's actually a file
        const stats = fs.statSync(filePath);
        if (!stats.isFile()) {
            return { isValid: false, error: ERROR_CODES.INVALID_FILE_PATH };
        }

        return { isValid: true };
    } catch (error) {
        return { isValid: false, error: ERROR_CODES.FILE_NOT_FOUND };
    }
}

/**
 * Checks if a file exists without creating it.
 * @param filePath - Path to the file
 * @returns Validation result
 */
export function validateFileExists(filePath: string): ValidationResult {
    try {
        if (!filePath) {
            return { isValid: false, error: ERROR_CODES.INVALID_FILE_PATH };
        }

        if (!fs.existsSync(filePath)) {
            return { isValid: false, error: ERROR_CODES.FILE_NOT_FOUND };
        }

        const stats = fs.statSync(filePath);
        if (!stats.isFile()) {
            return { isValid: false, error: ERROR_CODES.INVALID_FILE_PATH };
        }

        return { isValid: true };
    } catch (error) {
        return { isValid: false, error: ERROR_CODES.FILE_NOT_FOUND };
    }
}

/**
 * Checks if a directory exists without creating it.
 * @param dirPath - Path to the directory
 * @returns Validation result
 */
export function validateDirectoryExists(dirPath: string): ValidationResult {
    try {
        if (!dirPath) {
            return { isValid: false, error: ERROR_CODES.INVALID_DIRECTORY_PATH };
        }

        if (!fs.existsSync(dirPath)) {
            return { isValid: false, error: ERROR_CODES.DIRECTORY_NOT_FOUND };
        }

        const stats = fs.statSync(dirPath);
        if (!stats.isDirectory()) {
            return { isValid: false, error: ERROR_CODES.INVALID_DIRECTORY_PATH };
        }

        return { isValid: true };
    } catch (error) {
        return { isValid: false, error: ERROR_CODES.DIRECTORY_NOT_FOUND };
    }
}

/**
 * Generate the absolute path from the directory volume and the file name.
 * Ensures the directory exists.
 * @param directoryVolumeName - Directory volume name
 * @param fileName - File name
 * @returns Absolute file path
 */
export function generatePath(directoryVolumeName: string, fileName: string): FilePath {
    const absolutePath = path.join(directoryVolumeName, fileName);
    ensureDirectoryExists(directoryVolumeName);
    return absolutePath;
}