#############################################################################
# MIT License

# Copyright (c) 2017 Mats Julian Olsen

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice
# shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
import random

"""
Functions for generating the cyclic group [0,...n-1]. Use instead of
random.shuffle() or similar.

Functions:
    pyudorandom(n) <- generate the numbers in 0,...n-1
    bin_gcd(a, b) <- calculate the gcd of a and b fast

"""


def items(ls):
    """
    Yields the elements of ls in a pseudorandom fashion.

    """
    num = len(ls)
    if num == 0:
        return
    for i in indices(num):
        yield ls[i]


def shuffle(ls):
    """
    Takes a list ls and returns a new list with the elements of ls
    in a new order.

    """
    return list(items(ls))


def indices(n):
    """
    Generates the cyclic group 0 through n-1 using a number
    which is relative prime to n.

    """
    rand = find_gcd_one(n)
    i = 1
    while i <= n:
        yield i * rand % n
        i += 1


def find_gcd_one(n):
    """
    Find a number between 1 and n that has gcd with n equal 1.

    """
    while True:
        rand = int(random.random() * n)
        if bin_gcd(rand, n) == 1:
            return rand


def bin_gcd(a, b):
    """
    Return the greatest common divisor of a and b using the binary
    gcd algorithm.

    """
    if a == b or b == 0:
        return a
    if a == 0:
        return b

    if not a & 1:
        if not b & 1:
            return bin_gcd(a >> 1, b >> 1) << 1
        else:
            return bin_gcd(a >> 1, b)
    if not b & 1:
        return bin_gcd(a, b >> 1)
    if a > b:
        return bin_gcd((a - b) >> 1, b)

    return bin_gcd((b - a) >> 1, a)
