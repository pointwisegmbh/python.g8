from setuptools import setup

setup(
        name="$name$",
        version="1.0.0",
        author="$author$",
        author_email="$author_email$",
        description="$desc$",
        scripts=[
            "$main_script_name$.py"
        ],
        install_requires=[
        ],
        packages=["$name;format="word"$"],
        include_package_data=True
)
