# Installation

## Download the package
First you need to clone this repo. In your terminal run the following:

````bash 
git clone https://github.com/SNEWS2/SNEWS_Coincidence_System.git
cd SNEWS_Coincidence_System
pip install ./ --user
````

## install the package
### 1) Using Poetry

If you don't already have poetry installed (check with `poetry --version`), install it:
```bash
pip3 install poetry
```

Poetry relies on the `pyproject.toml` file for project-related stuff, so be sure to issue the following poetry commands from the same directory with that file (i.e., the root directory of this codebase).



To run in the virtual environment, you can either enter its shell directly from the terminal
(`poetry shell`, then `exit` to deactivate the environment), or you can run individual commands like
so: `poetry run <command>`.

For example, to run `snews_cs`, you can do ether:
```bash
# Poetry will execute everything after "run" in the virtual environment
poetry run snews_cs --help
```

or (if you still want to activate/deactivate your venv)

```bash
poetry shell
# Note: Your prompt will now include text like this (snews-cs-py3.11)
# to indicate that you are in the virutal environment.

# Run the program
snews_cs --help

# Leave virtual environment; return to original shell
exit
```

## 2) Using setuptools with virtualenv

First install `virtualenv` if you don't already have it: `pip3 install virtualenv`.

Then create a virtual environment and install the package into it:
````bash
# Create an empty virtual environment
virtualenv venv

# Enter the virtual environment shell
source venv/bin/activate

# Install the package
pip install ./ --user
````

### Notes

For more details, and for developers, see the [Developer Notes](./docs/user/Developers.md) page.
