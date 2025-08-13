/**
 * @file resp.h
 * @brief Implementation of Redis RESP-2 protocol.
 * 
 * This file contains the definitions for serializing and deserializing RESP-2 protocol messages.
 * RESP is the Redis Serialization Protocol used for client-server communication.
 */

#ifndef RESP_H
#define RESP_H

#include <string>
#include <vector>
#include <optional>

namespace resp {

/**
 * @enum Type
 * @brief Enumeration for RESP data types.
 */
enum class Type {
    SimpleString,  ///< Simple string prefixed with '+'
    Error,         ///< Error prefixed with '-'
    Integer,       ///< Integer prefixed with ':'
    BulkString,    ///< Bulk string prefixed with '$'
    Array          ///< Array prefixed with '*'
};

/**
 * @class Value
 * @brief Class representing a RESP-2 value.
 * 
 * This class provides methods to create, serialize, and deserialize RESP-2 values.
 * It supports all RESP-2 data types, including Simple Strings, Errors, Integers,
 * Bulk Strings, and Arrays.
 */
class Value {
public:
    /**
     * @brief Create a RESP Simple String.
     * @param str The string value.
     * @return A new Value object representing a Simple String.
     */
    static Value createSimpleString(const std::string& str);
    
    /**
     * @brief Create a RESP Error.
     * @param message The error message.
     * @return A new Value object representing an Error.
     */
    static Value createError(const std::string& message);
    
    /**
     * @brief Create a RESP Integer.
     * @param value The integer value.
     * @return A new Value object representing an Integer.
     */
    static Value createInteger(int64_t value);
    
    /**
     * @brief Create a RESP Bulk String.
     * @param str The string value.
     * @return A new Value object representing a Bulk String.
     */
    static Value createBulkString(const std::string& str);
    
    /**
     * @brief Create a RESP Null Bulk String.
     * @return A new Value object representing a Null Bulk String.
     */
    static Value createNullBulkString();
    
    /**
     * @brief Create a RESP Array.
     * @param values The array values.
     * @return A new Value object representing an Array.
     */
    static Value createArray(const std::vector<Value>& values);
    
    /**
     * @brief Create a RESP Null Array.
     * @return A new Value object representing a Null Array.
     */
    static Value createNullArray();
    
    /**
     * @brief Get the type of this value.
     * @return The type of the value.
     */
    Type getType() const;
    
    /**
     * @brief Get the string value.
     * @return The string value if this is a string type, or std::nullopt otherwise.
     */
    std::optional<std::string> getString() const;
    
    /**
     * @brief Get the integer value.
     * @return The integer value if this is an integer type, or std::nullopt otherwise.
     */
    std::optional<int64_t> getInteger() const;
    
    /**
     * @brief Get the array values.
     * @return The array values if this is an array type, or std::nullopt otherwise.
     */
    std::optional<std::vector<Value>> getArray() const;
    
    /**
     * @brief Check if this is a null value.
     * @return True if this is a null value, false otherwise.
     */
    bool isNull() const;
    
    /**
     * @brief Serialize this value to a RESP-2 message.
     * @return The serialized RESP-2 message as a string.
     */
    std::string serialize() const;
    
    /**
     * @brief Deserialize a RESP-2 message.
     * @param data The serialized RESP-2 message.
     * @param[out] consumed The number of bytes consumed during deserialization.
     * @return The deserialized Value object, or std::nullopt if deserialization fails.
     */
    static std::optional<Value> deserialize(const std::string& data, size_t& consumed);

private:
    Type type_;                        ///< The type of the RESP value.
    bool null_ = false;                ///< Indicates whether this is a null value.
    std::string string_value_;         ///< The string value for string types.
    int64_t integer_value_ = 0;        ///< The integer value for integer type.
    std::vector<Value> array_values_;  ///< The array values for array type.
    
    /**
     * @brief Constructor.
     * @param type The type of the RESP value.
     */
    explicit Value(Type type);
};

} // namespace resp

#endif // RESP_H