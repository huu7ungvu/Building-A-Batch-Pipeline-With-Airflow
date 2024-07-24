from setuptools import find_packages, setup

setup(
    name='my_modules',
    version='0.0.1',
    description='',
    packages=find_packages(),
    zip_safe=False
    # install_requires=[
    #     # Thêm các yêu cầu cần thiết tại đây
    # ],
)
# Phải đặt tên file là setup + đặt folder hợp lý trước khi init vào pipeline beam