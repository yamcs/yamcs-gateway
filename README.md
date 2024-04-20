Yamcs Gateway

The goal of this project is to allow Yamcs to control instruments/payloads as part of an EGSE. 

EGSE, short for Electrical Ground Support Equipment, is a common term in the aerospace industry denoting specialized equipment and systems essential for supporting, testing, and maintaining the electrical systems and subsystems of spacecraft, satellites, and similar vehicles. These EGSE systems often utilize low-level hardware connections such as MIL1553, CANbus, RS422, among others.

Yamcs abstracts the concept of data links for connecting to a target system, with most links implementing network protocols such as TCP or UDP. Integrating links that utilize low-level hardware connections poses challenges in Java, often requiring the use of device drivers available only in C.

In projects where Yamcs is used as part of the EGSE, a common approach involves developing interfacing software that communicates with Yamcs via standard TCP network connections and controls the target system using custom-built protocols. The Yamcs Gateway serves as a framework facilitating the implementation of such interface software.

Rust has been selected as the language for implementing the Yamcs Gateway due to its high-level nature, memory safety features, and its ease of interfacing with C libraries.

The directory structure includes:

* ygw: This serves as the primary Rust library implementing the Yamcs Gateway, and it's available on crates.io for easy access.
* ygw-macros: This directory contains implementations of macros designed to streamline the creation of Yamcs parameters and commands.
* ygw-plugin: Here lies a Yamcs plugin, which operates within Yamcs itself and communicates with the Gateway via TCP.
* quickstart: An illustrative example program showcasing the utilization of the ygw crate to generate an executable.


