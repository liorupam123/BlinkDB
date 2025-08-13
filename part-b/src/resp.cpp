/**
 * @file resp.cpp
 * @brief Implementation of Redis RESP-2 protocol.
 *
 * This file provides the implementation of the RESP (REdis Serialization Protocol) used
 * for communication between the client and server. It includes serialization and
 * deserialization of RESP data types such as Simple Strings, Errors, Integers, Bulk Strings,
 * and Arrays.
 */

 #include "resp.h"
 #include <sstream>
 #include <string>
 
 namespace resp {
 
 // Constants
 /**
  * @brief Carriage return and line feed sequence.
  */
 const char CRLF[] = "\r\n";
 
 /**
  * @brief Prefix for RESP Simple Strings.
  */
 const char SIMPLE_STRING_PREFIX = '+';
 
 /**
  * @brief Prefix for RESP Errors.
  */
 const char ERROR_PREFIX = '-';
 
 /**
  * @brief Prefix for RESP Integers.
  */
 const char INTEGER_PREFIX = ':';
 
 /**
  * @brief Prefix for RESP Bulk Strings.
  */
 const char BULK_STRING_PREFIX = '$';
 
 /**
  * @brief Prefix for RESP Arrays.
  */
 const char ARRAY_PREFIX = '*';
 
 // Value implementation
 
 /**
  * @brief Constructs a Value object with the specified type.
  * @param type The type of the RESP value.
  */
 Value::Value(Type type) : type_(type) {}
 
 /**
  * @brief Creates a RESP Simple String value.
  * @param str The string content.
  * @return A Value object representing a Simple String.
  */
 Value Value::createSimpleString(const std::string& str) {
     Value value(Type::SimpleString);
     value.string_value_ = str;
     return value;
 }
 
 /**
  * @brief Creates a RESP Error value.
  * @param message The error message.
  * @return A Value object representing an Error.
  */
 Value Value::createError(const std::string& message) {
     Value value(Type::Error);
     value.string_value_ = message;
     return value;
 }
 
 /**
  * @brief Creates a RESP Integer value.
  * @param value The integer value.
  * @return A Value object representing an Integer.
  */
 Value Value::createInteger(int64_t value) {
     Value val(Type::Integer);
     val.integer_value_ = value;
     return val;
 }
 
 /**
  * @brief Creates a RESP Bulk String value.
  * @param str The string content.
  * @return A Value object representing a Bulk String.
  */
 Value Value::createBulkString(const std::string& str) {
     Value value(Type::BulkString);
     value.string_value_ = str;
     return value;
 }
 
 /**
  * @brief Creates a RESP Null Bulk String value.
  * @return A Value object representing a Null Bulk String.
  */
 Value Value::createNullBulkString() {
     Value value(Type::BulkString);
     value.null_ = true;
     return value;
 }
 
 /**
  * @brief Creates a RESP Array value.
  * @param values The array elements.
  * @return A Value object representing an Array.
  */
 Value Value::createArray(const std::vector<Value>& values) {
     Value value(Type::Array);
     value.array_values_ = values;
     return value;
 }
 
 /**
  * @brief Creates a RESP Null Array value.
  * @return A Value object representing a Null Array.
  */
 Value Value::createNullArray() {
     Value value(Type::Array);
     value.null_ = true;
     return value;
 }
 
 /**
  * @brief Gets the type of the RESP value.
  * @return The type of the value.
  */
 Type Value::getType() const {
     return type_;
 }
 
 /**
  * @brief Gets the string content of the RESP value, if applicable.
  * @return The string content or std::nullopt if not applicable.
  */
 std::optional<std::string> Value::getString() const {
     if (type_ == Type::SimpleString || type_ == Type::Error || type_ == Type::BulkString) {
         if (null_) return std::nullopt;
         return string_value_;
     }
     return std::nullopt;
 }
 
 /**
  * @brief Gets the integer content of the RESP value, if applicable.
  * @return The integer content or std::nullopt if not applicable.
  */
 std::optional<int64_t> Value::getInteger() const {
     if (type_ == Type::Integer) {
         return integer_value_;
     }
     return std::nullopt;
 }
 
 /**
  * @brief Gets the array content of the RESP value, if applicable.
  * @return The array content or std::nullopt if not applicable.
  */
 std::optional<std::vector<Value>> Value::getArray() const {
     if (type_ == Type::Array) {
         if (null_) return std::nullopt;
         return array_values_;
     }
     return std::nullopt;
 }
 
 /**
  * @brief Checks if the RESP value is null.
  * @return True if the value is null, false otherwise.
  */
 bool Value::isNull() const {
     return null_;
 }
 
 /**
  * @brief Serializes the RESP value into a string.
  * @return The serialized RESP string.
  */
 std::string Value::serialize() const {
     std::ostringstream oss;
     
     switch (type_) {
         case Type::SimpleString:
             oss << SIMPLE_STRING_PREFIX << string_value_ << CRLF;
             break;
             
         case Type::Error:
             oss << ERROR_PREFIX << string_value_ << CRLF;
             break;
             
         case Type::Integer:
             oss << INTEGER_PREFIX << integer_value_ << CRLF;
             break;
             
         case Type::BulkString:
             if (null_) {
                 oss << BULK_STRING_PREFIX << "-1" << CRLF;
             } else {
                 oss << BULK_STRING_PREFIX << string_value_.length() << CRLF
                     << string_value_ << CRLF;
             }
             break;
             
         case Type::Array:
             if (null_) {
                 oss << ARRAY_PREFIX << "-1" << CRLF;
             } else {
                 oss << ARRAY_PREFIX << array_values_.size() << CRLF;
                 for (const auto& value : array_values_) {
                     oss << value.serialize();
                 }
             }
             break;
     }
     
     return oss.str();
 }
 
 /**
  * @brief Deserializes a RESP value from a string.
  * @param data The serialized RESP string.
  * @param consumed The number of characters consumed during deserialization.
  * @return The deserialized Value object or std::nullopt if deserialization fails.
  */
 std::optional<Value> Value::deserialize(const std::string& data, size_t& consumed) {
     if (data.empty()) {
         return std::nullopt;
     }
     
     consumed = 0;
     char type = data[0];
     size_t pos = 1;
     
     // Helper to find CRLF
     auto findCRLF = [&data, &pos]() -> std::optional<size_t> {
         size_t crlf = data.find("\r\n", pos);
         if (crlf == std::string::npos) {
             return std::nullopt;
         }
         return crlf;
     };
     
     // Parse based on type prefix
     switch (type) {
         case SIMPLE_STRING_PREFIX: {
             auto crlf = findCRLF();
             if (!crlf) return std::nullopt;
             
             std::string str = data.substr(pos, *crlf - pos);
             consumed = *crlf + 2; // +2 for CRLF
             return createSimpleString(str);
         }
             
         case ERROR_PREFIX: {
             auto crlf = findCRLF();
             if (!crlf) return std::nullopt;
             
             std::string str = data.substr(pos, *crlf - pos);
             consumed = *crlf + 2;
             return createError(str);
         }
             
         case INTEGER_PREFIX: {
             auto crlf = findCRLF();
             if (!crlf) return std::nullopt;
             
             std::string num_str = data.substr(pos, *crlf - pos);
             consumed = *crlf + 2;
             try {
                 int64_t num = std::stoll(num_str);
                 return createInteger(num);
             } catch (...) {
                 return std::nullopt;
             }
         }
             
         case BULK_STRING_PREFIX: {
             auto crlf = findCRLF();
             if (!crlf) return std::nullopt;
             
             std::string len_str = data.substr(pos, *crlf - pos);
             int64_t len;
             try {
                 len = std::stoll(len_str);
             } catch (...) {
                 return std::nullopt;
             }
             
             pos = *crlf + 2; // Skip CRLF
             
             // Null bulk string
             if (len == -1) {
                 consumed = pos;
                 return createNullBulkString();
             }
             
             // Check if we have enough data
             if (pos + len + 2 > data.length()) {
                 return std::nullopt;
             }
             
             std::string str = data.substr(pos, len);
             pos += len;
             
             // Make sure we have CRLF
             if (data.substr(pos, 2) != "\r\n") {
                 return std::nullopt;
             }
             
             consumed = pos + 2;
             return createBulkString(str);
         }
             
         case ARRAY_PREFIX: {
             auto crlf = findCRLF();
             if (!crlf) return std::nullopt;
             
             std::string len_str = data.substr(pos, *crlf - pos);
             int64_t len;
             try {
                 len = std::stoll(len_str);
             } catch (...) {
                 return std::nullopt;
             }
             
             pos = *crlf + 2; // Skip CRLF
             
             // Null array
             if (len == -1) {
                 consumed = pos;
                 return createNullArray();
             }
             
             std::vector<Value> values;
             for (int64_t i = 0; i < len; i++) {
                 if (pos >= data.length()) {
                     return std::nullopt;
                 }
                 
                 size_t element_consumed = 0;
                 auto element = deserialize(data.substr(pos), element_consumed);
                 if (!element) {
                     return std::nullopt;
                 }
                 
                 values.push_back(*element);
                 pos += element_consumed;
             }
             
             consumed = pos;
             return createArray(values);
         }
             
         default:
             return std::nullopt;
     }
 }
 
 } // namespace resp