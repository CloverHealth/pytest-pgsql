Contributing Guide
==================

Setup
~~~~~

Set up your development environment with::

    git clone git@github.com:CloverHealth/pytest-pgsql.git
    cd pytest-pgsql
    make setup

``make setup`` will setup a virtual environment managed by `pyenv <https://github.com/yyuu/pyenv>`_ and install dependencies.

Note that if you'd like to use something else to manage dependencies other than pyenv, call ``make dependencies`` instead of
``make setup``.

Testing and Validation
~~~~~~~~~~~~~~~~~~~~~~

Run the tests with::

    make test

Validate the code with::

    make validate

Documentation
~~~~~~~~~~~~~

`Sphinx <http://www.sphinx-doc.org/>`_ documentation can be built with::

    make docs

The static HTML files are stored in the ``docs/_build/html`` directory. A shortcut for opening them on OSX is::

    make open_docs

Releases and Versioning
~~~~~~~~~~~~~~~~~~~~~~~

Anything that is merged into the master branch will be automatically deployed to PyPI.
Documentation will be published to `ReadTheDocs <https://readthedocs.org>`_ soon.

The following files will be generated and should *not* be edited by a user:

* ``ChangeLog`` - Contains the commit messages of the releases. Please have readable commit messages in the
  master branch and squash and merge commits when necessary.
* ``AUTHORS`` - Contains the contributing authors.
* ``version.py`` - Automatically updated to include the version string.

This project uses `Semantic Versioning <http://semver.org>`_ through `PBR <https://docs.openstack.org/developer/pbr/>`_. This means when you make a commit, you can add a message like::

    sem-ver: feature, Added this functionality that does blah.

Depending on the sem-ver tag, the version will be bumped in the right way when releasing the package. For more information,
about PBR, go the the `PBR docs <https://docs.openstack.org/developer/pbr/>`_.
