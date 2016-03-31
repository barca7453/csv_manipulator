#include <iostream>
#include <cstring>
#include <getopt.h>
#include "util.h"
#include "col_compute.h"

void ShowUsage(){
      cerr << "Usage: csv COMPUTE "
          << "Options:\n"  
          << "\t-i,--input <FileName>\t\tInput CSV file\n"
          << "\t-o,--output <FileName>\t\t Result of computation go into this file.\n"
          << "\t-e,--expr <col_name><*,+,-,/><col_name> \t\tSpecify the compute expressioni\t\t\n"
          << "\t-h,--with_header \t\tThere is a header present in the input file\t\t\n"
          << std::endl;
}

void ShowJoinUsage(){
      cerr << "Usage: csv JOIN "
          << "Options:\n"  
          << "\t-l,--left <FileName>\t\tInput CSV file\n"
          << "\t-r,--right <FileName>\t\t Result of computation go into this file.\n"
          << "\t-u,--left_col <col_name> \t\tSpecify the join col name in the left file expressioni\t\t\n"
          << "\t-v,--right_col <col_name> \t\tSpecify the join col name in the right file\t\t\n"
          << "\t-o,--output_file <FileName> \t\tSpecify the output file name.\t\t\n"
          << "\t-h,--with_header \t\tThere is a header present in the input file\t\t\n"
          << "\t-t,--type <type> \t\tSpecify join type inner or outer, default is inner.\t\t\n"
          << std::endl;
}


 
int main(int argc, char **argv) {

///////// OPTION PROCESSING////

  int c;
  
  // This tool has 2 main categories
  // COMPUTE and JOIN
  if (argc < 2) {
      ShowUsage();
      ShowJoinUsage();
      exit(0);
  }

  if (!strcmp(argv[1], "JOIN")) {

    string lf;
    string rf;
    string of;
    string lc;
    string rc;
    string type;
    bool has_header = false;
    bool is_outer_join = false;
 
    while (1) {
      static struct option long_options[] =
        {
          {"left_file", required_argument, 0, 'l'},
          {"right_file", required_argument, 0, 'r'},
          {"output_file", required_argument, 0, 'o'},
          {"left_col", required_argument, 0, 'u'},
          {"right_col", required_argument, 0, 'v'},
          {"join", required_argument, 0, 'j'},
          {"with_header", no_argument, 0, 'h'},
          {0,0,0,0},
        };
      /* getopt_long stores the option index here. */
      int option_index = 0;


      c = getopt_long (argc, argv, "l:r:o:u:v:hj:",
                       long_options, &option_index);

      /* Detect the end of the options. */
      if (c == -1)
        break;

      switch (c) {
        case 'l':
          lf = optarg;
          break;

        case 'r':
          rf = optarg;
          break;

        case 'u':
          lc = optarg;
          break;

        case 'v':
          rc = optarg;
          break;

        case 'o':
          of = optarg;
          break;

       case 'j':
          type = optarg;
          
          if (type == "outer"){
              is_outer_join = true;
          }

       case 'h':
          has_header = true;
          break;

         break;

        default:
          ShowJoinUsage();
          abort ();
        }
    }

    if (lf.empty()) {
        cerr << "Specify left file\n";
        exit(0);
    } else if (rf.empty()) {
        cerr << "Specify right file\n";
        exit(0);
    } else if (of.empty()) {
        cerr << "Specify output file \n";
        exit(0);
    } else if (lc.empty()) {
        cerr << "SPecify left-column name -lc. \n";
        exit (0);
    } else if (rc.empty()) {
        cerr << "Specify right column in the join -rc \n";
        exit(0);
    } else if (type.empty()) {
        cerr << "Specify Join type, otherwise the tool will assume its an inner join\n";
        exit(0);
    }

    csv::compute::CSVCompute::Join(lf, rf, of, lc, rc, has_header,is_outer_join);
  } else if (!strcmp(argv[1], "COMPUTE")) {

      string input_file; 
      string output_file;
      string compute_exp;
      string filter_exp;
      string with_header;
      bool has_header = false;
 
      while (1) {
          static struct option long_options[] =
          {
              {"expr", required_argument, 0, 'e'},
              {"filter", required_argument, 0, 'f'},
              {"input", required_argument, 0, 'i'},
              {"output", required_argument, 0, 'o'},
              {"with_header", no_argument, 0, 'h'},
              {0,0,0,0},
          };
          /* getopt_long stores the option index here. */
          int option_index = 0;

          c = getopt_long (argc, argv, "e:f:j:i:o:h",
              long_options, &option_index);

          /* Detect the end of the options. */
          if (c == -1)
              break;

          switch (c) {
              case 'e':
                  compute_exp = optarg;
                  break;

              case 'f':
                  filter_exp = optarg;
                  break;

              case 'i':
                  input_file = optarg;
                  break;

              case 'o':
                  output_file = optarg;
                  break;

              case 'h':
                  has_header = true;
                  break;

              default:
                  cerr << "Usage: "
                      << "Options:\n"  
                      << "\t-i,--input <FileName>\t\tInput CSV file\n"
                      << "\t-o,--output <FileName>\t\t Result of computation go into this file.\n"
                      << "\t-e,--expr <col_name><*,+,-,/><col_name> \t\tSpecify the compute expressioni\t\t\n"
                      << "\t-h,--with_header \t\tThere is a header present in the input file\t\t\n"
                      << std::endl;
                  exit (0);
          }
      }

      if (compute_exp.empty()) {
          cerr << "Specify compute expression. \n";
          exit(0);
      } else if (input_file.empty()) {
          cerr << "Specify input file. \n";
          exit(0);
      } else if (output_file.empty()) {
          cerr << "Specify output file \n";
          exit(0);
      }
      // Perform evaluation on the CSV file.
      csv::compute::CSVCompute::Evaluate(input_file, compute_exp, filter_exp, output_file, has_header);
  } else {
      cerr << "Specify either JOIN or COMPUTE\n";
      ShowUsage();
      ShowJoinUsage();
      exit(0);
  }

////////////////////////////////

 return 0;
}

