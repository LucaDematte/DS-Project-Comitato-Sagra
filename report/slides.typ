#import "@preview/polylux:0.4.0": *
#import "@preview/metropolis-polylux:0.1.0" as metropolis
#import "@preview/chronos:0.3.0"

#import metropolis: new-section, focus

#show: metropolis.setup.with(
  text-font: "Fira Sans",
  math-font: "Fira Math",
  code-font: "Fira Code",
  text-size: 23pt,
  footer: [],
)

#slide[
  #set page(header: none, footer: none, margin: 3em)

 
  #text(size: 1.3em)[
    *Distributed Systems Project*
  ]

  Group: _Comitato Sagra_

  #metropolis.divider
  
  #set text(size: .8em, weight: "light")
  Enrico Dalla Croce, Luca Demmaté

  University of Trento

  07 July, 2026
]

#slide[
  = Outline

  #metropolis.outline
]

#new-section[Design]

#slide[
  = Ask System

  
]

#new-section[See it in action]

#slide[
  = Update Protocol

  #chronos.diagram({
    import chronos: *
    _par("Alice")
    _par("Bob")
  })
]

#slide[
  = Election

  animazione con cosa succede
]
