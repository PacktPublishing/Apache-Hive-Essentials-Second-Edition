#!/usr/bin/env python
'''
This is a script to upper all cases
'''
import sys

def main():
    try:
        for line in sys.stdin:
          n = line.strip()
          print n.upper()
    except:
        return None


if __name__ == "__main__":main()