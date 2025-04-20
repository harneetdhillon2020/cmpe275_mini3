## Mini 3 - Design a replication elgorithm

The third installment of mini explorations is giving you an opportunity
to practice what you have learned in the first minis and studied in class.
You are in control of the design, and given some general guidelines. 

Construct a network of servers/processes across multiple hosts to provide
an replication (NWR) algorithm that adapts to network and load pressures. 

Good luck!


## Algorithmic Design

Your algorithm is a major extension of your engineering understanding, 
constructing servers (processes), and interacts and cooperations to solve
problems (not just simple micro-servers). Consider carefully the design and
variables that go into your work and how well it performs. Consider the 
class discussion (to be or past depending on when mini3 is released) talking 
about factors that influence a sharing algorithm and weighting.

If we define an equation where a sum of variables (x) and its weight (c) is 
used to create a scalar score (rank), we could represent it as: 

      Rank = c0x0 + c1x1 + c2x2 … + cixi

Where factors (cixi) can be derived from environmental conditions or 
internal:

   * Number of messages that can be stolen
   * Number of messages enqueued
   * Threshold (floor) to disallow stealing, a minimum number
   * Number of times a message can be stolen? 
   * Distance (max hops) a message can be stolen from (see slide 80)
   * Or distance in which subparts can be separated (elasticity/repulsion 
     of dispersion)
   * Delay (t) in which repeated stealing can occur
   * Capacity of server and/or network
       ** Processor loads, memory, CPU utilization, time
       ** floors and ceilings caps
   * Time – FIFO or a gradient-based approach like LRU
   * Type of message, sender priority, etc.
   * Programming language sensitivities?

How do you know which conditions are key contributors to an algorithmic 
design?


### Technical

We know from our discussions that a random or round-robin approach are 
too basic so please do not target these approaches Hint: The focus is 
not on data, like mini 1 and 2 rather, on routing efficacies and recovery.

Like previous minis, no Java, JavaScript, or JS derivatives


### Considerations

**Fairness.** Rate of equalization (how fast can the algorithm stabilize 
an imbalance?). Does your algorithm equalize quickly (heartbeats)?

**Proof and verification.** How do you prove your algorithm works? Under 
what conditions does it does not work?

**Consistency vs performance.** Does eventually consistent apply? How 
do you meassure it? Be warned, resolving conflicts matter in successful
usage/acceptance.


## Scaling

The testing of your algorithm should include the understanding how it 
performs under stress and failure. Consider the following:

**Weak Scaling.** How does your algorithm keep up under a rapid increase 
in demain (load), or under sustained load (low, medium, high).

**Strong Scaling.** What happens as you add more resources (servers, 
memory, threads) while maintaining constant (sustained) loads. Does the
responsiveness or throughput increase?

A note on scaling: There is a misconseption that if we add resources there 
is a one-to-one gain in performance (e.g., doubling resources, doubles 
performance). At times this is true at initial scaling though at higher 
numbers, scaling starts to have less of an effect, it can flatten (no gain), 
or actually decrease.  More often than not, the gain is a fraction of the 
increase (< 1.0). In rare cases, the performance gain can be greather than 
the percent increase. For instance, going from 2 servers to 4 servers 
(doubling, 100%), could net a performance increate greater than 100%.

Measuring benefits is very project specific.  It many cases it comes down 
to the costs ($, time, people), and others it is weigthed strongly by 
external perception: faster, more availablty, or more features. In other 
words, for some a 25% gain on doubling the number of servers is a success, 
and for others it is a failure.


## Additional Comments

Please refer to the notes (notes.pdf) with hints and examples 
regarding mini 3. Note and lecture slides provide direction to construct 
a network of servers 

In order to provide as much opportunity for success, the first of several 
presentations on setting up multiple servers has been uploaded. The examples 
are incomplete and require work on your parts to fully implement your servers. 

### Time management

The algorithm is applied on top of a network of servers. Both can be worked 
on concurrently.

If you get stuck on a coding issue, do not spend half a day in isolation 
trying to figure it out. Contact the group (discord) if you are having 
problems after a couple hours.

### Adhoc

   * Mini3 is not about breath of functionaliy so, focus on what is important.
   * Python might be an easier choice if C++ is a concern, but be aware of GIL 
     limiations
   * Example code can be found in the gRPC examples directory from GitHub or 
     the grpc-loop lab
   * Remember latencies of services (gRPC) in your work. Your code must run 
     as separate servers (not different URLs, or testing harness/tool).

Validation should run with at least 5 servers (2 computers) to prove the 
algorithm's design. I would recommend that your team run the servers as 
scripts, not within an IDE. Use multiple terminals and bash scripts to start 
the servers.

There is always a potiential for honors points. This is an award on merit.

