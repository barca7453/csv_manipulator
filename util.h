#ifndef __CSV_UTIL
#define __CSV_UTIL

#include <string>
#include <algorithm>
#include <memory>
#include <iostream>
#include <fstream>
#include <sstream> 
#include <vector>
#include <stdio.h>
#include <cassert>

using namespace std;
namespace csv { namespace util {

bool invalidChar (char c)
{ 
    return !isprint((unsigned)c); 
}
 
// Split into vector of strings
void split(const string& str, char delimiter, vector<int>& res) {
   stringstream ss(str); // Turn the string into a stream.
   string tok;
   while(getline(ss, tok, delimiter)) {
      tok.erase(remove_if(tok.begin(),tok.end(), invalidChar), tok.end());
      res.push_back(atoi(tok.c_str())); // This should be a templated method XXX HACK TODO
   }
}

// Split into vector of ints
void split(const string& str, char delimiter, vector<string>& res) {
   stringstream ss(str); // Turn the string into a stream.
   string tok;
   while(getline(ss, tok, delimiter)) {
      tok.erase(remove_if(tok.begin(),tok.end(), invalidChar), tok.end());
      res.push_back(tok); // This should be a templated method XXX HACK TODO
   }
}

// Vector of strings representing
// the column names allowed through
// the filter..
struct SimpleStringFilter {
    SimpleStringFilter ()
    {}

    SimpleStringFilter (string filter_exp) {
        // Parse exp
        // populate col vector
        split(filter_exp, ',', col_vec_);
    }

    bool Allow (const string& col_name) {
        // No filter specified, all columns allowed.
        if (col_vec_.empty()) {
            return true;
        }

        vector<string>::iterator iter = find(col_vec_.begin(),
                                          col_vec_.end(),
                                          col_name);
        return (iter != col_vec_.end());
    }

private:
    vector<string> col_vec_;
};



// File header defining column names
// If there is no header defined, default column names are assigned
struct Header {

    void set (const std::string header_str) {
        vector<string> header;
        std::stringstream ss(header_str);

        split(header_str, ',', header);
        for (auto &i: header) {
            i.erase(remove_if(i.begin(),i.end(), invalidChar), i.end()); 
            header_.push_back(make_pair(i,true));
        }
    }
    
    // Create a header with default column names in the format
    // col_<index>
    void MakeHeader (const string& file_name) {
        std::ifstream csv_file(file_name.c_str(), std::ifstream::in);
        if (csv_file.is_open()) {
          string record;
          std::getline(csv_file, record);
       
          vector<string> header;
          split(record, ',', header);
          vector<string>::iterator iter = header.begin();
          for (;iter != header.end(); ++iter) {
              int col_index = std::distance (header.begin(), iter);
              string col_name = "col_" + to_string(col_index);
              pair<string, bool> column;
              column.first = col_name;
              column.second = true;
              header_.push_back (column);
          }
        }
    }

    // mark columns according to the filter definition
    void ApplyFilter (SimpleStringFilter& filter) {
       vector<pair<string,bool>>::iterator iter_hdr = header_.begin();
        for (;iter_hdr != header_.end(); ++iter_hdr) {
            string col_name = (*iter_hdr).first;
            if (!filter.Allow (col_name)) {
                MarkColumnFilter(std::distance(header_.begin(), iter_hdr));
            }
        }
    }

    // Make this an empty header. 
    void Clear() {
        header_.clear();
    }

    // Getters
    int GetColumnIndex(const std::string& col_name) {
        std::vector<pair<std::string, bool>>::iterator it = header_.begin();

        for (;it != header_.end(); ++it) {
           if (it->first == col_name) {
               return (std::distance (header_.begin(), it));
           }
        }

        return -1;
    }

    const std::string GetColumnName(const size_t& col_index) {
        if (col_index >= GetNumCols()){
            return std::string();
        }

        return (header_[col_index].first);
    }

    vector<pair<string,bool>> GetColumnVector () {
        return header_;
    }

    int GetNumCols() const {
        return header_.size();
    }

    string GetHeaderString() const {
        stringstream str;
        if (header_.size() == 0) {
            return "";
        }

        for (auto &i:header_) {
           pair<string, bool> column = i;
           if (column.second){
               str << column.first << "," ;
           }
        }

        std::string header_string = str.str();
        header_string.pop_back();
        return header_string;
    }

    // Modifiers
    void AddColumn (const std::string name) {
        header_.push_back(make_pair(name,true));
    }


    // Mark column as filtered out
    // Specify the col_index instead of name
    // since there can be duplicat col names.e
    void MarkColumnFilter (const int col_index) {
        assert (col_index <= header_.size());

        header_[col_index].second = false;
    }


private:
    vector<pair<std::string,bool>> header_;
};


template <typename T>
struct CSVRecord
{
   CSVRecord ()
   {}

   // Create a dummy record filled with 
   // the specified filler value.
   // Useful in outer joins.
   CSVRecord (Header& header, T filler)
       : header_(header)
   {
       for (auto &i:header_.GetColumnVector()){
           data_.push_back(make_pair(filler,true));
       }
   }

   CSVRecord (Header& header, const SimpleStringFilter& filter = SimpleStringFilter("")) :
       header_(header), filter_(filter)
    {}

   // Getters
   T const& operator[](std::size_t index) const {
       return data_[index].first;
   }

   std::size_t size() const {
       return data_.size();
   }

   Header& GetHeader() {
       return header_;
   }

   vector<pair<T,bool>> GetRecordColumnVector() {
        return data_;
   }

   // fetches the next line from the incoming
   // stream, construct the row
   void NextRecord (std::istream& str) {
       std::string line;
       std::getline(str,line);

       std::stringstream lineStream(line);
       std::string cell_str;

       data_.clear();

       while(std::getline(lineStream,cell_str,',')) {
           data_.push_back(make_pair(atoi(cell_str.c_str()),true));
       }
   }

  // Copy incoming row into this row
  CSVRecord& operator=(CSVRecord rhs) {
      if (this != &rhs) {
          // Clean out data_
          data_.clear();
          // Clean out header_
          header_.Clear();

          size_t col_index = 0;
          for (auto &i:rhs.GetRecordColumnVector()) {
              T col_value = i.first;

              string rhs_col_name = rhs.GetHeader().GetColumnName(col_index);
              AddColumn (rhs_col_name, col_value);
              col_index++;
          }
      }
      return *this;
   }


  // APPEND the columns from the rhs to this
  // A,B,C + A,B,C = A,B,C,A,B,C
   CSVRecord operator+(CSVRecord rhs) {
       size_t col_index = 0;
       for (auto &i:rhs.GetRecordColumnVector()) {
           T col_value = i.first;

           string rhs_col_name = rhs.GetHeader().GetColumnName(col_index);
           AddColumn (rhs_col_name, col_value);
           col_index++;
       }
       return *this;
   }


   // Join 2 records
   CSVRecord Join (CSVRecord record,
                    const string& col_name_left,
                    const string& col_name_right,
                    bool is_outer = false) {
       int index_col_left = header_.GetColumnIndex(col_name_left);
       int index_col_right = record.GetHeader().GetColumnIndex(col_name_right);


       CSVRecord join_result;
       if (data_[index_col_left].first == record[index_col_right]){
           // Add left table columns.
           for (auto &i : header_.GetColumnVector()) {
               string col_name = i.first;
               int col_index = header_.GetColumnIndex(col_name);
               if (col_index < 0) {
                   cerr << "Could not find column " << col_name << " in the file.\n" ;
                   exit(0);
               }
               join_result.AddColumn(col_name, data_[col_index].first);
           }

           // Add right table columns.
           for (auto &i : record.GetHeader().GetColumnVector()) {
               string col_name = i.first;
               int col_index = record.GetHeader().GetColumnIndex(col_name);
               if (col_index < 0) {
                   cerr << "Could not find column " << col_name << " in the file.\n" ;
                   exit(0);
               }
 
               join_result.AddColumn(col_name, record[col_index]);
           }
       } 

       return join_result;
   }

   // Remove column from header and record 
   // if filtered out.
   void ApplyFilter() {
      vector<pair<string,bool>> column_vector = header_.GetColumnVector(); 
      vector<pair<string,bool>>::const_iterator iter_hdr = column_vector.begin();
      for (;iter_hdr != column_vector.end(); ++iter_hdr) {
          string col_name = (*iter_hdr).first;
          if (!filter_.Allow (col_name)) {
              MarkColumnFilter(header_.GetColumnIndex(col_name));
          }
      }

      // Now that the cells are gone
      // filter the header.
      header_.ApplyFilter(filter_);
   }

   // Return CSV output of the
   // record.
   const std::string GetRecordString() {
      // Mark columns from the record and header to be filtered out.
      if (data_.empty()){
          return "";
      }

      // Apply filter if any specified
      ApplyFilter(); 

      typename vector<pair<T,bool>>::const_iterator iter = data_.begin();
      stringstream record;
      for (;iter != data_.end(); ++iter){
          pair<T,bool> copy = (*iter);
          if (copy.second) {
              record << copy.first << ",";
          }
      }

      std::string record_string = record.str();
      record_string.pop_back();
      return record_string;
   }

   // Add column name to the header
   // Add data column to the row data
   void AddColumn (const std::string name, const T& value) {
       data_.push_back(make_pair(value,true));
       header_.AddColumn(name);
   }

   // Mark the cell to be filtered out.
   // True = Can eb displayed
   // False = Can NOT be displayed
   void MarkColumnFilter (const int col_index) {
       assert (col_index <= data_.size());

       data_[col_index].second = false;
   }

   void PrintColumnStack(){
       for (auto &i:data_) {
           cout << i.first << " " << i.second << endl;
       }
   }

private:
   std::vector<pair<T,bool>> data_;
   Header header_;
   SimpleStringFilter filter_;
};


std::istream& operator>>(std::istream& str, CSVRecord<int>& data) {
   data.NextRecord(str);
   return str;
}

// Faux Generic data converter, ideally
// use lexical_cast, but was not supposed
// to use boost
template <typename T>
struct Converter {
  static  int convert (const string& in) {
      return atoi(in.c_str());
  }
};


} } //namespace





#endif
