#include <vector>
#include <iostream>
#include <string.h>

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

namespace py = pybind11;
const int PARTICLE_NUM_VALUES = 14;

namespace JointCharArray {
    using CharArray = std::vector<char *>;

    class JointCharArray {
        private:
        int counter;
        CharArray arrays;
        char* cycle_arrays(char* cursor, bool fake_counter=false) {
            int counter = this->counter;
            if (counter < this->arrays.size()-1) {
                counter += 1;
                cursor = this->arrays[counter];
            }
            if (!fake_counter) {
                this->counter = counter;
            }
            return cursor;
        }

        public:
        JointCharArray(CharArray &arrays) {
            this->counter = 0;
            this->arrays = arrays;
        };

        char* reset(bool fake_counter=false) {
            if (!fake_counter) {
                this->counter = 0;
            }
            return this->arrays[this->counter];
        }

        char* nextline(char* cursor, bool fake_counter=false) {
            cursor = std::strstr(cursor, "\n") + 1;
            if (*(cursor) != '\0') {
                return cursor;
            }
            return this->cycle_arrays(cursor, fake_counter);
        }

        char* strstr(char* cursor, const char* needle, bool fake_counter=false) {
            do {
                cursor = std::strstr(cursor, needle);
                if (cursor) {
                    break;
                }
                cursor = this->cycle_arrays(cursor, fake_counter);
            } while (cursor);
            return cursor;
        }
    };

}

char* skip_seps(char* from, const char* sep) {
    char* end = from;
    for (;*end == *sep;++end); // skip spaces before first char of the next token
    return end;
}

char* skip_token(char* from, const char* sep) {
    char* end = from;
    end = skip_seps(end, sep);
    for (;*end != *sep;++end); // skip current token
    end = skip_seps(end, sep);
    return end;
}


std::tuple<py::array, py::str> parse_batch(const int num_particles, char* input_buf, char* input_batch) {
    std::string output_buf;
    int cur_num_particles;
    int num_events = 0;
    double weight;

    std::vector<double>* res = new std::vector<double>();

    JointCharArray::CharArray input_batches = {input_buf, input_batch};
    JointCharArray::JointCharArray ja(input_batches);
    char* cursor = ja.reset();
    cursor = ja.strstr(cursor, "<event");
    while (cursor && *cursor != '\0') {
        if (!ja.strstr(cursor, "</event>", true)) {
            output_buf = std::string(cursor);
            break;
        }
        cursor = ja.nextline(cursor);  // local cursor at meta
        cursor = skip_seps(cursor, " ");

        cur_num_particles = strtol(cursor, &cursor, 10);
        //std::cout << "Num particles: " << cur_num_particles;

        if (num_particles != cur_num_particles) {
            //std::cout << std::endl << "Num particles mismatch (" << cur_num_particles << "!= " << num_particles << "). Skipping" << std::endl;
            cursor = ja.strstr(cursor, "<event");
            continue;
        }

        cursor = skip_token(cursor, " ");  // skip second col
        weight = strtod(cursor, &cursor);
        //std::cout << " Weight: " << weight << std::endl;

        cursor = ja.nextline(cursor);  // line with first particle

        res->reserve(num_particles*PARTICLE_NUM_VALUES);
        for (int p = 0; p < num_particles; ++p) {
            for (int i = 0; i < PARTICLE_NUM_VALUES-1; i++) {
                cursor = skip_seps(cursor, " ");  // skip second col
                double quantity = strtod(cursor, &cursor);
                res->push_back(quantity);
            }
            res->push_back(weight);
            cursor = ja.nextline(cursor);
        }
        num_events += 1;
        //std::cout << num_events << std::endl;
    }

    auto capsule = py::capsule(res, [] (void *v) {delete reinterpret_cast<std::vector<double>*>(v);});
    auto res_arr =  py::array({num_events, num_particles, PARTICLE_NUM_VALUES},
                              res->data(), capsule);
    auto res_str = py::str(output_buf);
    return {res_arr, res_str};
}

PYBIND11_MODULE(fastlhe, m) {
    m.doc() = "fast lhe format utils"; // optional module docstring

    m.def("parse_batch", &parse_batch, "Parse extract from lhe into numpy array");
}
