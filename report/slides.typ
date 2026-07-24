#import "@preview/polylux:0.4.0": *
#import "@preview/metropolis-polylux:0.1.0" as metropolis
#import metropolis: new-section, focus

#show: metropolis.setup.with(
  text-font: "Fira Sans",
  math-font: "Fira Math",
  code-font: "Fira Code",
  text-size: 23pt,
  footer: [My cool footer], // defaults to none
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

  07 July, 2026
]

#slide[
  = Agenda

  #metropolis.outline
]

#new-section[My first section]

#slide[
  = The Fundamental Theorem of Calculus

  For $f = (dif F) / (dif x)$ we _know_ that
  $
    integral_a^b f(x) dif x = F(b) - F(a)
  $

  See `https://en.wikipedia.org/wiki/Fundamental_theorem_of_calculus`
]

#slide[
  slide without a title
]

#new-section[My second section]

#slide[
  = Heron algorithm

  ```julia
  function heron(x)
      r = x
      while abs(r^2 - x) > eps()
          r = (r + x / r) / 2
      end
      return r
  end

  @test heron(42) ≈ sqrt(42)
  ```
]

#slide[
  #show: focus
  Something very important
]
