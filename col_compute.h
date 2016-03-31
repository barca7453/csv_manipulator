#ifndef _COL_COMPUTE_
#define _COL_COMPUTE_

#include <deque>
#include <fstream>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <string>
#include <assert.h>
#include "util.h"

using namespace std;
namespace csv { namespace compute { 

// COMPUTE expression specified by A+B 
// Stack representation of the expression  ex:
// 1 (A)
// +
// 2 (B)
template <typename T>
struct StackExpression {
    StackExpression (csv::util::CSVRecord<T>& record,
                     string& expression)
    {
        // 2 columns with an operator in the middle
        // Search for */+-
        // choose the operands from either side of the
        // operator
        std::vector<std::string> elems;
        std::size_t found = expression.find('+');
        if (found != string::npos) { 
            expr_stack_.push_back("+");
            csv::util::split(expression, '+', elems);
        } else if ((found = expression.find('-')) != string::npos) {
            expr_stack_.push_back("-");
            csv::util::split(expression, '-', elems);
        } else if ((found = expression.find('*')) != string::npos) {
            expr_stack_.push_back("*");
            csv::util::split(expression, '*', elems);
        } else if ((found = expression.find('/')) != string::npos) {
            expr_stack_.push_back("/");
            csv::util::split(expression, '/', elems);
        } 

        int indexA = record.GetHeader().GetColumnIndex(elems[0]);
        if (indexA < 0) {
            cerr <<"Column " << elems[0] << " was not found. Did you specify -h and not have a header in the file?\n";
            exit(0);
        }

        int indexB = record.GetHeader().GetColumnIndex(elems[1]);
        if (indexB < 0) {
            cerr <<"Column " << elems[1] << " was not found. Did you specify -h and not have a header in the file?\n";
            exit(0);
        }

        expr_stack_.push_front(to_string(record[indexA]));
        expr_stack_.push_back(to_string(record[indexB]));

    }
    
    // Execute the rule specified in the stack
    // return the result.
    T Collapse() {
        T result = 0;
        result = csv::util::Converter<T>::convert ((expr_stack_.front()).c_str());
        expr_stack_.pop_front();
        char oper = *(expr_stack_.front().c_str());
        expr_stack_.pop_front();
        switch (oper) {
            case '+':
                result += csv::util::Converter<T>::convert ((expr_stack_.front()).c_str());
                expr_stack_.pop_front();
                break;
            case '*':
                result *= csv::util::Converter<T>::convert ((expr_stack_.front()).c_str());
                expr_stack_.pop_front();
                break;
            case '-':
                result -= csv::util::Converter<T>::convert ((expr_stack_.front()).c_str());
                expr_stack_.pop_front();
                break;
            case '/':
                result /= csv::util::Converter<T>::convert ((expr_stack_.front()).c_str());
                expr_stack_.pop_front();
                break;
            default:
                break;
        }

        return result;
    }
    
private:
    deque<string> expr_stack_;
};

// Single expression stack to evaluate any expression
// *** This should be templatized further to a policy
// based template accepting different types of Expressions
// In this case its the StackExpression, it could be many others
template <typename T>
struct ExpressionEvaluator {
    ExpressionEvaluator (shared_ptr<StackExpression<T>> e) {
        result_ = e->Collapse();
    }

    T get_result() const {
        return result_;
    }
private:
    T result_;
};

// A CSVRecord, which is a row of the csv file
// is evaluated here.
template <typename T>
struct ColExprEval {

  ColExprEval(csv::util::CSVRecord<T>& rec) : rec_(rec){}

  csv::util::CSVRecord<T> eval (string& expression){
      shared_ptr<StackExpression<T>> expr_stack = make_shared<StackExpression<T>> (rec_, expression);
      shared_ptr<ExpressionEvaluator<T>> evaluator = make_shared<ExpressionEvaluator<T>>(expr_stack);

      T col_result = evaluator->get_result();
      rec_.AddColumn("result", col_result);// Add the result to the new columnin the row

      return rec_;
  }

private:
  vector<T> row_elems_;
  csv::util::CSVRecord<T> rec_;

};

// Evaluator
// Bunch of static methods carrying out the main 
// chunk of the work.
struct CSVCompute {
    static void Evaluate(string& input_file_name,
                         string& compute_expression,
                         string& filter_expression,
                         string& output_file_name,
                         bool has_header = false) {

        ifstream csv_file_read(input_file_name.c_str(), std::ifstream::in);
        ofstream csv_file_write(output_file_name);

        csv::util::Header header;
        if (csv_file_read.is_open()) {
            // read the column names if they are defined
            if (has_header) {
                string header_str;
                // Get the header line
                std::getline(csv_file_read, header_str);
                header.set (header_str);
            } else {
                // No column header defined
                // Generate default col names
                header.MakeHeader (input_file_name);
            }

            // Read the rest of the file
            csv::util::SimpleStringFilter filter(filter_expression);
            csv::util::CSVRecord<int> record(header, filter);
            bool header_written = false;
            // stream every line into a record.
            while (csv_file_read >> record) {
                // Do the desired computation on columns
                std::shared_ptr<ColExprEval<int>> c = make_shared<ColExprEval<int>>(record);
                csv::util::CSVRecord<int> result = c->eval(compute_expression);
                if (!header_written) {
                    csv_file_write << result.GetHeader().GetHeaderString() << endl;
                    header_written = true;
                }
                csv_file_write << result.GetRecordString() << endl;
            }
        }
    }

    // INNER/OUTER JOINS
    static void Join (string& left_file_name,
                      string& right_file_name,
                      string& output_file_name,
                      string& col_name_left,
                      string& col_name_right,
                      bool has_header = false,
                      bool is_outer = false) {
        ifstream left_file_read(left_file_name.c_str(), std::ifstream::in);
        ifstream right_file_read(right_file_name.c_str(), std::ifstream::in);
        ofstream output_file_write(output_file_name);

        csv::util::Header header_left;
        if (left_file_read.is_open()) {
            // read the column names if they are defined
            if (has_header) {
                string header_str;
                // Get the header line
                std::getline(left_file_read, header_str);
                header_left.set (header_str);
            } else {
                // No column header defined
                // Generate default col names
                header_left.MakeHeader (left_file_name);
            }
        }

        csv::util::Header header_right;
        if (right_file_read.is_open()) {
            // read the column names if they are defined
            if (has_header) {
                string header_str;
                // Get the header line
                std::getline(right_file_read, header_str);
                header_right.set (header_str);
            } else {
                // No column header defined
                // Generate default col names
                header_right.MakeHeader (right_file_name);
            }
        }
        right_file_read.close(); 

        // Join loop n^2 SimpleJoin
        csv::util::CSVRecord<int> record_left(header_left);
        bool header_written = false;
        while (left_file_read >> record_left) {
            csv::util::CSVRecord<int> record_right(header_right);
            // reset stream of the "right" file
            ifstream right_file_join_read(right_file_name.c_str(), std::ifstream::in);
            // Skip the header if specified
            if (has_header) {
                string header_str;
                // Get the header line
                std::getline(right_file_join_read, header_str);
            }
 
            bool match = false;
            while (right_file_join_read >> record_right) {
                csv::util::CSVRecord<int> result = record_left.Join(record_right, col_name_left, col_name_right);

                if (!header_written) {
                    output_file_write << result.GetHeader().GetHeaderString() << endl;
                    header_written = true;
                }
                
                string result_str = result.GetRecordString();
                if (!result_str.empty()) {
                    match = true;
                    output_file_write << result.GetRecordString() << endl;
                }
            }

            // If there is no match in the rhs file
            // add a NULL record in its place for a
            // left outer join result.
            if (!match && is_outer) {
                csv::util::CSVRecord<int> result;
                csv::util::CSVRecord<int> empty_right(header_right,0);
                csv::util::CSVRecord<int> left = record_left;
                result = left + empty_right;

                if (!header_written) {
                    output_file_write << result.GetHeader().GetHeaderString() << endl;
                    header_written = true;
                }
                
                string result_str = result.GetRecordString();
                if (!result_str.empty()) {
                    output_file_write << result.GetRecordString() << endl;
                }
            }
            match = false;
            right_file_join_read.close();
        }
    }
};

} } //namespace
#endif
