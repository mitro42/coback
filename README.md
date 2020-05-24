# coback

[![Build Status](https://travis-ci.com/mitro42/coback.svg?branch=master)](https://travis-ci.com/mitro42/coback)
[![Go Report Card](https://goreportcard.com/badge/github.com/mitro42/coback)](https://goreportcard.com/report/github.com/mitro42/coback)
[![codecov](https://codecov.io/gh/mitro42/coback/branch/master/graph/badge.svg)](https://codecov.io/gh/mitro42/coback)

Leave no file behind! Make no decision twice!

Coback is a simple tool to consolidate your backups and save you some headache.

## The Problem

For years I struggled to keep my photo collection organized. I never had a single location where I knew for sure that all my photos and videos were there. Some of my albums were backed up in many places where some of them were lying forgotten in some folder like G:/backups/from_drive_E/temp/backup/photos... waiting to be found again.

My collection spread out over dozens of hard drives, CDs, DVDs, SSDs, memory cards and USD sticks. I tried to sort out the mess many times but always stopped halfway through for some reason, and I ended up in a worse situation because now I had yet another copy of some of my files, but not all of them.

## The Tool

I needed a tool to solve this, so I designed CoBack. The main guiding requirements were these:

- Should be easy to use yet powerful enough to solve the problem

  This one is pretty obvious.

- I don't want to make decision about a file twice

  If I've already seen a photo and placed it in my collection, the tool should just ignore it in the future and not bother me with it again.
  If I've deleted it the same. The tool should know that I don't want to keep that file.

- Resumable process

  I knew that even with the tool organizing the collection will take a long time. There will be interruptions, change in priorities, life in general. I wanted a tool that makes it trivial to pick it up where I left off, no matter if I was interrupted for just a minute or in a coma for two years.

## How it works

CoBack is a very simple to use command line tool. It analyzes the folders you give it and copies the files you haven't seen yet to a new location. From there you put the files you want to keep to your collection and delete the rest. Then repeat with a new folder until you're done.

## QNFABUKA (Questions Not Frequently Asked But Useful to Know the Answers to)

- Is it only for photos and videos?

No, of course not. This is what it was designed for and what it was mostly tested with. But for CoBack the files are just series of bytes, it doesn't matter if they are photos, PDFs, MP3s or other.

- What similarity measures are used?

CoBack only uses bitwise comparison and md5 sums. So if two files contain the same image but have a slightly different white balance, or were just simply saved with different compression settings will be treated as completely different files. This is a major limitation of the tool now and would be nice to fix in the future.
