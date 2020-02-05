include(FindPackageHandleStandardArgs)

find_package(PkgConfig)
pkg_check_modules(PC_Pqxx QUIET libpqxx)

find_path(Pqxx_INCLUDE_DIR
    NAME pqxx
    PATHS /usr/include /usr/local/include ${PC_Pqxx_INCLUDE_DIRS})

find_library(Pqxx_LIBRARY NAMES pqxx PATHS ${PC_Pqxx_LIBRARY_DIRS})

find_package_handle_standard_args(Pqxx DEFAULT_MSG Pqxx_LIBRARY Pqxx_INCLUDE_DIR)

if(Pqxx_FOUND)
  set(Pqxx_LIBRARIES ${Pqxx_LIBRARY})
  set(Pqxx_INCLUDE_DIRS ${Pqxx_INCLUDE_DIR})
  set(Pqxx_DEFINITIONS ${PC_Pqxx_CFLAGS_OTHER})
endif()

mark_as_advanced(Pqxx_INCLUDE_DIR Pqxx_LIBRARY)
