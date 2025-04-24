## Language Syntax

### Create Data Classes for Custom Data Objects

Python code frequently has classes for _data objects_: items that exist to store values, but do not carry out actions. If you are creating classes for data objects in your Python code, consider using either [Pydantic](https://docs.pydantic.dev/) or the built-in [data classes](https://docs.python.org/3/library/dataclasses.html) feature.

Pydantic provides validation, serialization and other features for data objects. You need to define the classes for Pydantic data objects with type hints.

The built-in syntax for data classes offers fewer capabilities than Pydantic. The data class syntax enables you to reduce the amount of code that you need to define data objects, and data classes also provide a limited set of features, such as the ability to mark instances of a data class as [frozen](https://docs.python.org/3/library/dataclasses.html#frozen-instances). Each data class acts as a standard Python class, because the syntax for data classes does not change the behavior of the classes that you define with it.

> [PEP 557](https://www.python.org/dev/peps/pep-0557/) describes data classes.

### Use enum or Named Tuples for Immutable Sets of Key-Value Pairs

Use the _enum_ type for immutable collections of key-value pairs. Enums can use class inheritance.

Python also has _collections.namedtuple()_ for immutable key-value pairs. This feature was created before _enum_ types. Named tuples do not use classes.

### Format Strings with f-strings

The new [f-string](https://docs.python.org/3/reference/lexical_analysis.html#f-strings) syntax is both more readable and has better performance than older methods. Use f-strings instead of _%_ formatting, _str.format()_ or _str.Template()_.

The older features for formatting strings will not be removed, to avoid breaking backward compatibility.

> [PEP 498](https://www.python.org/dev/peps/pep-0498/) explains f-strings in detail.

### Use Datetime Objects with Time Zones

Always use _datetime_ objects that are [aware](https://docs.python.org/3/library/datetime.html?highlight=datetime#aware-and-naive-objects) of time zones. By default, Python creates _datetime_ objects that do not include a time zone. The documentation refers to _datetime_ objects without a time zone as **naive**.

Avoid using _date_ objects, except where the time of day is completely irrelevant. The _date_ objects are always **naive**, and do not include a time zone.

Use aware _datetime_ objects with the UTC time zone for timestamps, logs and other internal features.

To get the current time and date in UTC as an aware _datetime_ object, specify the UTC time zone with _now()_. For example:

Copy

```python
from datetime import datetime, timezone

dt = datetime.now(timezone.utc)

```

Our Python (3.10) includes the **zoneinfo** module. This provides access to the standard IANA database of time zones.

> [PEP 615](https://www.python.org/dev/peps/pep-0615/) describes support for the IANA time zone database with **zoneinfo**.

### Use collections.abc for Custom Collection Types

The abstract base classes in _collections.abc_ provide the components for building your own custom collection types.

Use these classes, because they are fast and well-tested. The implementations in Python 3.7 and above are written in C, to provide better performance than Python code.

### Use breakpoint() for Debugging

This function drops you into the debugger at the point where it is called. Both the [built-in debugger](https://docs.python.org/3/library/pdb.html) and external debuggers can use these breakpoints.

> [PEP 553](https://www.python.org/dev/peps/pep-0553/) describes the _breakpoint()_ function.

## Application Design

### Use Logging for Diagnostic Messages, Rather Than print()

The built-in _print()_ statement is convenient for adding debugging information, but you should include logging in your scripts and applications. Use the [logging](https://docs.python.org/3/library/logging.html#logrecord-attributes) module in the standard library.

### File Formats: TOML for Configuration & JSON for Data

Use [TOML](https://toml.io/) for data files that must be written or edited by human beings. Use the JSON format for data that is transferred between computer programs. If neither of these are suitable, consider using [KDL](https://kdl.dev/), which is an emerging standard.

Current versions of Python include support for reading and creating JSON. Python 3.11 and above include _tomllib_ to read the TOML format. If your Python software will generate TOML, you need to add [Tomli-W](https://pypi.org/project/tomli-w/) to your project.

Avoid using the INI or YAML formats for new projects. These formats are more difficult to validate with software, and human errors are also more likely.

> [PEP 680 - tomllib: Support for Parsing TOML in the Standard Library](https://peps.python.org/pep-0680/) explains why TOML is now included with Python.

### Only Use async Where It Makes Sense

The [asynchronous features of Python](https://docs.python.org/3/library/asyncio.html) enable a single process to avoid blocking on I/O operations. To achieve concurrency with Python, you must run multiple Python processes. Each of these processes may or may not use asynchronous I/O.

To run multiple application processes, either use a container system, with one container per process, or an application server like [Gunicorn](https://gunicorn.org/). If you need to build a custom application that manages multiple processes, use the [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) package in the Python standard library.

Code that uses asynchronous I/O must not call _any_ function that uses synchronous I/O, such as _open()_, or the _logging_ module in the standard library. Instead, you need to use either the equivalent functions from _asyncio_ in the standard library or a third-party library that is designed to support asynchronous code.

The [FastAPI](https://fastapi.tiangolo.com/) Web framework supports [using both synchronous and asynchronous functions in the same application](https://fastapi.tiangolo.com/async/). You must still ensure that asynchronous functions never call any synchronous function.

If you would like to work with _asyncio_, use Python 3.7 or above. Version 3.7 of Python introduced [context variables](https://docs.python.org/3/library/contextvars.html), which enable you to have data that is local to a specific _task_, as well as the _asyncio.run()_ function.

> [PEP 0567](https://www.python.org/dev/peps/pep-0567/) describes context variables.

## Libraries

### Handle Command-line Input with argparse

The [argparse](https://docs.python.org/3/library/argparse.html) module is now the recommended way to process command-line input. Use _argparse_, rather than the older _optparse_ and _getopt_.

The _optparse_ module is officially deprecated, so update code that uses _optparse_ to use _argparse_ instead.

Refer to [the argparse tutorial](https://docs.python.org/3/howto/argparse.html) in the official documentation for more details.

### Use pathlib for File and Directory Paths

Use [pathlib](https://docs.python.org/3/library/pathlib.html) objects instead of strings whenever you need to work with file and directory pathnames.

Consider using the [the pathlib equivalents for os functions](https://docs.python.org/3/library/pathlib.html#correspondence-to-tools-in-the-os-module).

Methods in the standard library support Path objects. For example, to list all of the the files in a directory, you can use either the _.iterdir()_ function of a Path object, or the _os.scandir()_ function.

This [RealPython article](https://realpython.com/working-with-files-in-python/#directory-listing-in-modern-python-versions) provides a full explanation of the different Python functions for working with files and directories.

### Use os.scandir() Instead of os.listdir()

The _os.scandir()_ function is significantly faster and more efficient than _os.listdir()_. If you previously used the _os.listdir()_ function, udate your code to use _os.scandir()_.

This function provides an iterator, and works with a context manager:

Copy

```python
import os

with os.scandir('some_directory/') as entries:
    for entry in entries:
        print(entry.name)

```

The context manager frees resources as soon as the function completes. Use this option if you are concerned about performance or concurrency.

The _os.walk()_ function now calls _os.scandir()_, so it automatically has the same improved performance as this function.

The _os.scandir()_ function was added in version 3.5 of Python.

> [PEP 471](https://www.python.org/dev/peps/pep-0471/) explains _os.scandir()_.

### Run External Commands with subprocess

The [subprocess](https://docs.python.org/3/library/subprocess.html) module provides a safe way to run external commands. Use _subprocess_ rather than shell backquoting or the functions in _os_, such as _spawn_, _popen2_ and _popen3_. The _subprocess.run()_ function in current versions of Python is sufficient for most cases.

> [PEP 324](https://www.python.org/dev/peps/pep-0324/) explains subprocess in detail.

### Use httpx for Web Clients

Use [httpx](https://www.python-httpx.org/) for Web client applications. Many Python applications include [requests](https://requests.readthedocs.io/en/latest/), but you should httpx for new projects.

The httpx package supersedes requests. It supports [HTTP/2](https://www.python-httpx.org/http2/) and [async](https://www.python-httpx.org/async/), which are not available with requests.

Avoid using _urllib.request_ from the Python standard library. It was designed as a low-level library, and lacks the features of requests and httpx.
