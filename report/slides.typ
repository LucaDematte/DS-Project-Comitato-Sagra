#import "@preview/polylux:0.4.0": *
#import "@preview/metropolis-polylux:0.1.0" as metropolis
#import "@preview/fletcher:0.5.8" as fletcher
#import "@preview/chronos:0.3.0"

#import metropolis: new-section, focus

#let sections-band = toolbox.all-sections( (sections, current) => {
  set text(fill: gray, size: .8em)
  sections
    .map(s => if s == current { strong(s) } else { s })
    .join([ • ])
})

#show: metropolis.setup.with(
  text-font: "Fira Sans",
  math-font: "Fira Math",
  code-font: "Fira Code",
  text-size: 23pt,
  footer: sections-band,
)

#let nodes = ("ID1", "ID2", "ID3", "ID4", "ID5", "ID6", "ID7")
#let edges = (
  (0, 1),
  (1, 2),
  (2, 3),
  (3, 4),
  (4, 5),
  (5, 6),
  (6, 0),
)

#slide[
  #set page(header: none, footer: none, margin: 3em)

  #text(size: 1.3em)[
    *Distributed Systems Project*
  ]

  Group: _Comitato Sagra_

  #metropolis.divider
  
  #set text(size: .8em, weight: "light")
  Enrico Dalla Croce, Luca Dematté

  University of Trento

  07 July, 2026
]

#slide[
  = Outline

  #metropolis.outline
]

#new-section[Ask System]

#slide[
  = ask()
  // #set align(center)
  
  #reveal-code(lines: (2, 3, 4, 5, 6, 7, 8))[
    ```java      
                 ⬇️​                        // type of the response
      askSystem.<CSUpdate>ask(            
        new CSWriteForward(...),          // message to be sent
        replicas.get(this.coordinatorId), // destination
        timeout,                          // timeout defined in ms
        (res, timedOut) -> {              // response handling:
           if (!timedOut) {...}           // - when the response arrives
           else {...}                     // - in case of timeout 
      }
    ```
  ]
]

#slide[
  = Example
  #show raw: set text(size: 16pt)

  #align(center)[
  #chronos.diagram({
    import chronos: *
    _par("Client")
    _par("contacted_replica", display-name: "Replica")
    _par("Coordinator")
    _par("Replica")

    _seq("Client", "contacted_replica", comment: [`WriteRequest`#super[#text(fill: green, `UUID`)]], comment-align: "center", enable-dst: true)
    _seq("contacted_replica", "Coordinator", comment: [`ForwardRequest`#super[#text(fill: red, `UUID`)]], comment-align: "center", enable-dst: true)
    _gap()
    _sync({
     _seq("Coordinator", "contacted_replica", comment: [`UPDATE`#super[#text(fill: red, `UUID`)]], comment-align: "center",)
     _seq("Coordinator", "Replica", comment: [`UPDATE`#super[#text(fill: blue, `UUID`)]], comment-align: "center", enable-dst: true)
    })
    _sync({
      _seq("contacted_replica", "Coordinator", comment: [`Ack`#super[#text(fill: red, `UUID`)]], comment-align: "center", dashed: true)
      _seq("Replica", "Coordinator", comment: [`ACK`#super[#text(fill: blue, `UUID`)]], comment-align: "center")
    })
    _gap()
    _sync({
      _seq("Coordinator", "contacted_replica", comment: [`WriteOk`#super[#text(fill: red, `UUID`)]], comment-align: "center", disable-src: true)
      _seq("Coordinator", "Replica", comment: [`WriteOk`#super[#text(fill: blue, `UUID`)]], comment-align: "center", disable-dst: true)
    })
    _seq("contacted_replica", "Client", comment: [`Ack`#super[#text(fill: green, `UUID`)]], comment-align: "center", dashed: true, disable-src: true)
  })
  ]
]

#new-section[Update Protocol]

#slide[
  = UUID to distinguish WriteRequest(s)
  
  #show raw: set text(size: 16pt)
  #align(center)[
  #chronos.diagram({
    import chronos: *
    _par("Replica A")
    _par("Coordinator")
    _par("Replica B")

    _note("right", [
      #set text(size: 10pt)
      #show raw: set text(size: 10pt)
      Generates UUID\ binding update with `<e,i>`
    ], pos: "Replica A")
    _sync({
      _seq("Replica A", "Coordinator", comment: [`ForwardRequest(2, 7,` #text(fill: red, `uuid`)`)`], comment-align: "center", enable-dst: true)
      _seq("Replica B", "Coordinator", comment: [`ForwardRequest(2, 7,` #text(fill: blue, `uuid`)`)`], comment-align: "center")
    })
    _gap()
    _note("right", [
      #set text(size: 10pt)
      #show raw: set text(size: 10pt)
      UUID receives\ matches
    ], pos: "Replica A")
    _sync({
     _seq("Coordinator", "Replica A", comment: [`UPDATE(<0,1>, 2, 7,` #text(fill: red, `uuid`)`)`], comment-align: "center")
     _seq("Coordinator", "Replica B", comment: [`UPDATE(<0,2>, 2, 7,` #text(fill: blue, `uuid`)`)`], comment-align: "center", disable-src: true)
    })
  })
  ]
]

#slide[
  = Overview

  #set text(size: 18pt)
  #show raw: set text(size: 16pt)
  #chronos.diagram({
    import chronos: *
    _par("Client")
    _par("contacted_replica", display-name: "Replica")
    _par("Coordinator")
    _par("Replica")

    _seq("Client", "contacted_replica", comment: [`(idx, val)`#super[#text(fill: green, `UUID`)]], comment-align: "center", enable-dst: true)
    _note("left", [
      #set text(size: 10pt)
      Generates\
      askUUID
    ])
    _note("right", [
      #set text(size: 10pt)
      #show raw: set text(size: 10pt)
      Generates UUID\ binding update with `<e,i>`
    ])
    _seq("contacted_replica", "Coordinator", comment: [`ForwardRequest(idx, val, uuid)`#super[#text(fill: red, `UUID`)]], comment-align: "center", enable-dst: true)
    _gap()
    _sync({
     _seq("Coordinator", "contacted_replica", comment: [`UPDATE(<e,i>, idx, val, uuid)`#super[#text(fill: red, `UUID`)]], comment-align: "center",)
     _seq("Coordinator", "Replica", comment: [`UPDATE(<e,i>, ...)`#super[#text(fill: blue, `UUID`)]], comment-align: "center", enable-dst: true)
    })
    _sync({
      _seq("contacted_replica", "Coordinator", comment: [`Ack`#super[#text(fill: red, `UUID`)]], comment-align: "center", dashed: true)
      _seq("Replica", "Coordinator", comment: [`ACK`#super[#text(fill: blue, `UUID`)]], comment-align: "center")
    })

    _gap()
    _sync({
      _seq("Coordinator", "contacted_replica", comment: [`WriteOk(<e,i>)`#super[#text(fill: red, `UUID`)]], comment-align: "center", disable-src: true)
      _seq("Coordinator", "Replica", comment: [`WriteOk(<e,i>)`#super[#text(fill: blue, `UUID`)]], comment-align: "center", disable-dst: true)
    })
    _note("over", [
      #set text(size: 10pt)
      #set align(center)
      Coordinator waits for\
      $floor(frac(N, 2, style: "horizontal"))+1$ACKs\
      from replicas\
      (included himself)
    ], pos: "Coordinator")
    
    _note("left", [
      #set text(size: 10pt)
      #show raw: set text(size: 10pt)
      Apply update `<e,i>`
    ], pos: "contacted_replica")
    _seq("contacted_replica", "Client", comment: [`Ack`#super[#text(fill: green, `UUID`)]], comment-align: "center", dashed: true, disable-src: true)
  })
]

#new-section[Ring Topology]

#slide[
  = Next
  
  #align(center)[
  #fletcher.diagram({
  	for (i, n) in nodes.enumerate() {
  		let θ = 90deg - i*360deg/nodes.len()
  		fletcher.node((θ, 150pt), n, stroke: 0.5pt, name: str(i), shape: circle)
  	}
  	for (from, to) in edges {
  		let bend = if (to, from) in edges { 10deg } else { 0deg }
  		fletcher.edge(label(str(from)), label(str(to)), "-|>", bend: bend, label: "next", label-angle: auto)
  	}
  })
  ]
]

#new-section[Election]

#focus[
  _Non Blocking Election_
  
  To make sure that the election process terminates we do _4_ things
]

#slide[
  = #numbering("I", 1) - Duplicated Election messages

  #set text(size: 18pt)
  #show raw: set text(14pt)
  #align(center)[
  #chronos.diagram({
    import chronos: *

    _sync({
      _seq("ID2", "ID3", comment: [`Election(ID2)`], comment-align: "center", destroy-dst: true)
      _seq("ID3", "ID4", comment: [`Election(ID3)`], comment-align: "center", destroy-dst: true)
      _seq("ID4", "ID6", comment: [`Election(ID4)`], comment-align: "center", destroy-dst: true)
      _seq("ID6", "ID7", comment: [`Election(ID6)`], comment-align: "center", destroy-dst: true)
    })
    _sync({
      _seq("ID3", "ID2", comment: [`Ack`], comment-align: "center", dashed: true)
      _seq("ID4", "ID3", comment: [`Ack`], comment-align: "center", dashed: true)
      _seq("ID6", "ID4", comment: [`Ack`], comment-align: "center", dashed: true)
      _seq("ID7", "ID6", comment: [`Ack`], comment-align: "center", dashed: true)
    })

    _gap()
    
    _seq("ID7", "ID2", comment: [`Election(ID7)`], comment-align: "center")
    _seq("ID2", "ID7", comment: [`Ack`], comment-align: "center", dashed: true)
    _seq("ID2", "ID3", comment: [`Election(ID7)`], comment-align: "center")
    _seq("ID3", "ID2", comment: [`Ack`], comment-align: "center", dashed: true)
    _seq("ID3", "ID4", comment: [`Election(ID7)`], comment-align: "center")
    _seq("ID4", "ID3", comment: [`Ack`], comment-align: "center", dashed: true)
    _seq("ID4", "ID6", comment: [`Election(ID7)`], comment-align: "center")
    _seq("ID6", "ID4", comment: [`Ack`], comment-align: "center", dashed: true)
    _seq("ID6", "ID7", comment: [`Election(ID7)`], comment-align: "center")
    _seq("ID7", "ID6", comment: [`Ack`], comment-align: "center", dashed: true)
  })
  ]
]

#slide[
  = #numbering("I", 2) - Next Crash
  
  #align(center)[
  #chronos.diagram({
    import chronos: *

    _seq("ID1", "ID2", comment: [`Election`], comment-align: "center", destroy-dst: true)
    _note("over", [
      #set text(size: 10pt)
      #show raw: set text(size: 10pt)
      Timed out
    ], pos: "ID1")
    _seq("ID1", "ID3", comment: [`Election`], comment-align: "center", destroy-dst: true)
    _note("over", [
      #set text(size: 10pt)
      #show raw: set text(size: 10pt)
      Timed out
    ], pos: "ID1")
    _seq("ID1", "ID4", comment: [`Election`], comment-align: "center")
    _seq("ID4", "ID1", comment: [`Ack`], comment-align: "center")
  })
  ]
]

#slide[
  = #numbering("I", 3) - Winner Crash
  #align(center)[
  #chronos.diagram({
    import chronos: *

    _sync({
      _seq("ID7", "ID1", comment: [`Election([7,1,2,3])`], comment-align: "center")
      _seq("ID1", "ID2", comment: [`Election([7,1,2,3])`], comment-align: "center", destroy-dst: true)
    })
    _gap()
    _note("over", [
      #set text(size: 12pt)
      #show raw: set text(size: 12pt)
      ID1 detects crash of its `next` which is the winner:\
      - removes it from the election message
      - computes new winner
      and then continues to circulate the message
    ], pos: "ID1")
    _gap()
    _seq("ID1", "ID3", comment: [`Election([7,1,3])`], comment-align: "center")
  })
  ]
]

#slide[
  = #numbering("I", 4) - Catch all solution

  Every replica start a timer at the start of every election.\
  If the timer expires the replica starts a new election attempt.

  In a similar fashion to when handling multiple replicas trying to start the election, only the most recent attempt is considered:

  ```java
  if(
    msg.getElectionAttempt() <= this.electionAttempt) {
    //drop election message
  } else {
    //accept election message
  }
  ```
]

#new-section[Miscellaneous]

#slide[
  = Non Blocking updates

  #show raw: set text(size: 23pt)
  
  During the election `WriteRequest` are still received\
  but processed when the election is over
]

#slide[
  = Crash Notice
  
  #align(center)[
  #chronos.diagram({
    import chronos: *
    _par("Replica A")
    _par("Coordinator")
    _par("Replica B")

    _seq("Replica A", "Coordinator", comment: [`ForwardRequest`], comment-align: "center", enable-dst: true)

    _gap()

    _sync({
     _seq("Coordinator", "Replica A", comment: [`UPDATE()`], comment-align: "center")
     _seq("Coordinator", "Replica B", comment: [`UPDATE()`], comment-align: "center")
    })
    _sync({
      _seq("Replica A", "Coordinator", comment: [`Ack`], comment-align: "center", dashed: true, disable-dst: true)
    })
    _note("over", [
      #set text(size: 10pt)
      #show raw: set text(size: 10pt)
      Replica B does not\ send the Ack
    ], pos: "Replica B")
    _note("over", [
      #set text(size: 10pt)
      #show raw: set text(size: 10pt)
      Coordinator detect crash
    ], pos: "Coordinator")
    _seq("Coordinator", "Replica A", comment: [`CrashNotice`], comment-align: "center")
  })
  ]
]

#slide[
  #set align(center)
  
  #fletcher.diagram({
  	for (i, n) in nodes.enumerate() {
  		let θ = 90deg - i*360deg/nodes.len()
  		fletcher.node((θ, 150pt), n, stroke: 0.5pt, name: str(i), shape: circle)
  	}
  	for (from, to) in edges {
  		let bend = if (to, from) in edges { 10deg } else { 0deg }
  		fletcher.edge(label(str(from)), label(str(to)), "-|>", bend: bend, label: "next", label-angle: auto)
  	}

   fletcher.edge(label("0"), label("3"),"-|>", label: "CrashNotice", label-angle: auto)
  })
]

#slide[
  #set align(center)
  #let edges = edges.slice(0, 3) + edges.slice(5, 7)
  
  #fletcher.diagram({
  	for (i, n) in nodes.enumerate() {
  		let θ = 90deg - i*360deg/nodes.len()
      if(n == "ID5") {
        fletcher.node((θ, 150pt), n, stroke: 0.5pt + red.lighten(50%), fill: red.lighten(50%), name: str(i), shape: circle)
      } else {
        fletcher.node((θ, 150pt), n, stroke: 0.5pt, name: str(i), shape: circle)  
      }
  	}
  	for (from, to) in edges {
  		let bend = if (to, from) in edges { 10deg } else { 0deg }
  		fletcher.edge(label(str(from)), label(str(to)), "-|>", bend: bend, label: "next", label-angle: auto)
  	}

   fletcher.edge(label("3"), label("5"),"-|>", label: "next", label-angle: auto)
  })
]

#new-section[Conclusions]

#slide[
  = Shower Thoughts
  - It was very interesting seeing how such a simple protocol can present so many problems in a distributed implementation.\
  - We spent a lot of time ironing out all edge cases we encountered, so we are confident about our implementation, but there might be more bugs that we missed.

  === About AskSystem
  It was very fun to build it. But definitely not necessary.\
  For sure we improved our knowledge of Java.
]

#focus[
  Thank you for your attention

  Questions?
]

#new-section[Extras]

#slide[
  = getNextOf()
  
  #show raw: set text(size: 16pt)
  
  #reveal-code(lines: (1, 2, 3, 4))[
    ```java
    private int getNextOf(int replicaId) {     replicaId=4
        return this.replicas.keySet().stream() [1] [2] [3] [4] [5] [6] [7]
                            .filter(id -> id > replicaId)      [5] [6] [7]
                            .min(Integer::compare)             [5]
                            .orElse(Collections.min(this.replicas.keySet()));
    }
    ```
  ]
]
