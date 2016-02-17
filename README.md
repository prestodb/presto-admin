# presto-admin

presto-admin installs, configures, and manages Presto installations.

Comprehensive documentation can be found [here](http://prestodb.github.io/presto-admin/).

## Requirements

1. Python 2.6 or 2.7
2. [Docker](https://www.docker.com/). (Only required for development, if you want to run the system tests)
    * If you DO NOT have Docker already installed, you can run the `install-docker.sh`
      script in the `bin` directory of this project. That script has only been tested on
      Ubuntu 14.04.
    * If you have Docker already installed, you need to make sure that your user has
      been added to the docker group. This will enable you to run commands without `sudo`,
      which is a requirement for some of the unit tests. To enable sudoless docker access
      run the following:
      
            $ sudo groupadd docker
            $ sudo gpasswd -a ${USER} docker
            $ sudo service docker restart
            
      If the user you added to the docker group is the same one you're logged in as, you will
      need to log out and back in so that the changes can take effect.

## Building

Presto-admin makes use of `make` as its build tool. `make` in turn calls out to various utilities (e.g.
`tox`, `flake8`, `sphinx-apidoc`, `python`) in order to perform the requested actions.

### Building the installer

The two tasks used to build the presto-admin installer are `dist` and 
`dist-offline`. The `dist` task builds an installer that requires internet 
connectivity during installation. The `dist-offline` task builds an installer
that does not require internet connectivity during installation. Instead the
offline installer downloads all dependencies at build time and points `pip` to 
those dependencies during installation. 

## License

Free software: Apache License Version 2.0 (APLv2).
