#include "storage_engine.h"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <unistd.h>
#include <limits.h>

using namespace std;

vector<string> tokenize(const string& input) {
    vector<string> tokens;
    istringstream iss(input);
    string token;
    while (iss >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

string extract_value(const string& input, const string& key) {
    size_t key_pos = input.find(key);
    if (key_pos == string::npos) return "";
    
    size_t value_start = key_pos + key.length();
    while (value_start < input.length() && isspace(input[value_start])) {
        value_start++;
    }
    
    return (value_start < input.length()) ? input.substr(value_start) : "";
}

string get_current_working_dir() {
    char cwd[PATH_MAX];
    return (getcwd(cwd, sizeof(cwd)) != nullptr) ? string(cwd) : ".";
}

int main() {
    string db_dir = get_current_working_dir() + "/blinkdb_data";
    cout << "Using database directory: " << db_dir << endl;
    
    StorageEngine engine(db_dir);
    
    cout << "BLINK DB REPL - Enhanced with WAL, Bloom Filters, and Parallel Compaction\n";
    cout << "Commands: SET <key> <value>, GET <key>, DEL <key>, SYNC, DEBUG, EXIT\n";
    
    for (string input; cout << "User> ", getline(cin, input);) {
        if (input.empty()) continue;
        
        vector<string> tokens = tokenize(input);
        if (tokens.empty()) continue;
        
        string command = tokens[0];
        for (auto& c : command) c = toupper(c);
        
        if (command == "SET") {
            if (tokens.size() < 3) {
                cout << "Error: SET requires a key and a value\n";
                continue;
            }
            string key = tokens[1];
            string value = extract_value(input, key);
            if (!engine.set(key.c_str(), value.c_str())) {
                cout << "Error setting value\n";
            }
        } else if (command == "GET") {
            if (tokens.size() < 2) {
                cout << "Error: GET requires a key\n";
                continue;
            }
            string key = tokens[1];
            optional<string> result = engine.get(key.c_str());
            cout << (result ? *result : "NULL") << endl;
        } else if (command == "DEL") {
            if (tokens.size() < 2) {
                cout << "Error: DEL requires a key\n";
                continue;
            }
            string key = tokens[1];
            if (!engine.del(key.c_str())) {
                cout << "Error deleting key\n";
            }
        } else if (command == "SYNC") {
            engine.sync();
            cout << "Database synchronized." << endl;
        } else if (command == "DEBUG") {
            engine.debug_print_tree();
        } else if (command == "EXIT" || command == "QUIT") {
            cout << "Exiting BLINK DB." << endl;
            break;
        } else {
            cout << "Unknown command.\n";
        }
    }
    
    return 0;
}