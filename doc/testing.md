

# Property based testing

I have had some exposure to property based testing via clojure [test.check](https://github.com/clojure/test.check) and through generating test data via clojure [spec](https://clojure.org/guides/spec). I was hoping that I would be able to use python [hypothesis](https://hypothesis.readthedocs.io) to start exploring some of those ideas here. However, generating test data via hypothesis was very slow. I had spent a few hours with hypothesis, but decided to abandon it. 

The big idea of [property based testing](https://read.klipse.tech/generative-testing-in-clojure-interactive-tutorial/) is that we precisely describe the properties about our function that hold true for all valid inputs. Then the _system_ can generate many, many valid inputs and determine whether the properties hold true. When a failing input is found, then system can narrow down that input to give the minimal failing case. 

Property based testing is in contrast to _example based testing_ where we give a series of precise example inputs and outputs. This is not either / or. Example based testing provides examples of how functions should be used with various types of inputs. However, spending time adding more and more examples quickly reaches a point of diminishing returns. We cannot necessarily be more confident in the correctness of our code just because we have 82 test cases rather than 20 test cases. I think that combining traditional example based testing with property based testing will be able to make us more confident in our code with minimal additional effort. So, I wasn't able to dig into hypothesis here, but I am planning to study it in the future. 

# Aside on Clojure
I love studying programming languages and I have a special fondness for the history and elegance of the Lisp family. [Clojure](https://clojure.org/) is a popular, modern version of Lisp. For those of us who spend most of our time with other languages, like Python, more important that the Clojure language is the Clojure inventor, [Rich Hickey](https://github.com/tallesl/Rich-Hickey-fanclub). All programmers should be familiar with the following talks:
* [Simple Made Easy](https://www.infoq.com/presentations/Simple-Made-Easy/)
* [Hammock Driven Development](https://www.youtube.com/watch?v=f84n5oFoZBc)
* [The Value of Values](https://www.infoq.com/presentations/Value-Values/)
* [Spec-ulation](https://www.youtube.com/watch?v=oyLBGkS5ICk)
* [Maybe Not](https://www.youtube.com/watch?v=YR5WdGrpoug)









