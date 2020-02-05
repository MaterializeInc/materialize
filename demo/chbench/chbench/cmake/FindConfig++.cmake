include(FindPackageHandleStandardArgs)

find_package(PkgConfig)
pkg_check_modules(PC_Config++ QUIET libconfig++)

find_path(Config++_INCLUDE_DIR
    NAME libconfig.h++
    PATHS /usr/include /usr/local/include ${PC_Config++_INCLUDE_DIRS})

find_library(Config++_LIBRARY NAMES config++ PATHS ${PC_Config++_LIBRARY_DIRS})

find_package_handle_standard_args(Config++ DEFAULT_MSG Config++_LIBRARY Config++_INCLUDE_DIR)

if(Config++_FOUND)
  set(Config++_LIBRARIES ${Config++_LIBRARY})
  set(Config++_INCLUDE_DIRS ${Config++_INCLUDE_DIR})
  set(Config++_DEFINITIONS ${PC_Config++_CFLAGS_OTHER})
endif()

mark_as_advanced(Config++_INCLUDE_DIR Config++_LIBRARY)
