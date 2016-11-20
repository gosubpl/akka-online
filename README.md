# akka-online
Online and streaming algorithms with Akka

In `org.apache.spark.streamdm` there is code copied from [Huawei Noah's Ark Lab streamDM](https://github.com/huawei-noah/streamDM) project (the licencse is Apache 2.0 too). The code has been adapted to work with Akka streams instead of Spark streaming module by removing dependencies on the Spark stuff. Tested path is the `HoeffdingTree` model usage, which works best with input sourced from `Arff` files via `SpecificationParser` / `ExampleParser` - see see other parts of `akka-online` for usage examples.
If you'd like to know more about HoeffdingTree on-line predictor, please read the [HDT docs](http://huawei-noah.github.io/streamDM/docs/HDT.html) or go to [Massive Online Analysis](http://moa.cms.waikato.ac.nz/) website that contains more information on the context.

Sample `Arff` files can be found in the [MOA dataset repository](http://moa.cs.waikato.ac.nz/datasets/).

For theoretical exposition to `HoeffdingTree` usage go to [the original paper](http://homes.cs.washington.edu/%7Epedrod/papers/kdd00.pdf).

In the `/lib` directory of the project I have put `suffixtree-1.0.0-SNAPSHOT.jar` which is a compiled artifact of the [Ukkonen's on-line Suffix Tree implementation](https://github.com/abahgat/suffixtree). The licensse for this project is also Apache 2.0.

In this project I also use Guava implementation of `Bloom Filter` (see project dependencies) but there are other [options](https://github.com/alexandrnikitin/bloom-filter-scala).
