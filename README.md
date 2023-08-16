# wos_builder

Create and populate a MySQL Database from Web of Science raw XML data

## Installation

The package is currently not listed on PyPi. You therefore need to install it from a local folder:

```
pip install ./wos_builder
```

## Getting Started

You can import the converter into your own Python project like this:

```{python}
from wos_builder.conversion import xml_to_sql
```

Alternatively, you can use the provided command line tool. Run the following command to see how to use it:

```
wos_xml_to_sql --help
```
