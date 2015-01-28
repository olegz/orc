/*
 * C09Adapter.hh
 *
 *  Created on: Jan 28, 2015
 *      Author: asandryhaila
 */

#ifndef C09ADAPTER_HH_
#define C09ADAPTER_HH_

#if __cplusplus < 201103L
  typedef unsigned long uint64_t;
  typedef long int64_t;
  #define unique_ptr auto_ptr
  #define nullptr NULL
  #define override

  namespace std {
    template<typename T>
    inline T move(T& x) { return x; }
  } // std


  /* Containers of unique_ptr<T> are replaced with std::vector<T*>
   * unique_ptr to arrays are replaced with std::vector
   * Unsupported containers (e.g. initializer_list) are replaced with std::vector
   * Rvalue references && are replaced by &
   * auto is replaced with appropriate data type
   */

#else
  #include <initializer_list>
  #include <array>

#endif // __cplusplus


#endif /* C09ADAPTER_HH_ */
