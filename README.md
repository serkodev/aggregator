# Aggregator

Aggregator is a batch processing library for Go. You can process and group up batch of tasks with keys in a single callback. Using it for grouping up database query or cache can help you to reduce loading of database and network.

## Features

- Support multi Aggregators (using `AggregatorList`) for fallback.
- Support multi workers to flush tasks.
- Support Go generics for query keys and result values.
- Support timeout-only or tasks limit-only.
- Support singleflight (using [serkodev/singleflight-any](https://github.com/serkodev/singleflight-any)).

## Requirement

Currently only Go 1.18 or above (with go generics), backward compatible is planned.

## Example

```go
callback := func(keys []string) (map[string]Book, error) {
    results := db.Query(`SELECT * FROM books WHERE name IN ?`, keys)
    return results, nil
}
agg, _ := aggregator.New(callback, 100*time.Millisecond, 5).Run()

for _, name := range []string{"foo", "foo", "bar", "baz", "baz"} {
    go func(n string) {
        book, err := agg.Query(n).Get()
        if err == nil {
            print(book.Name + ":" + book.Price, " ")
        }
    }(name)
}

// foo:10 foo:10 bar:25 baz:30 baz:30 
```

## How it works

```mermaid
flowchart LR;
    subgraph A [Aggregator]
        direction TB
        subgraph cb ["Customize Process (example)"]
        direction TB
            input("Input
            []string{#quot;foo#quot;, #quot;bar#quot;, #quot;baz#quot;}")
            db[("Database

            SELECT price FROM books<br />WHERE name
            IN ('foo', 'bar', 'baz')")]
            output("return map[string]int{
                &nbsp;&nbsp;&nbsp;&nbsp;#quot;foo#quot;: 10,
                &nbsp;&nbsp;&nbsp;&nbsp;#quot;bar#quot;: 25,
                &nbsp;&nbsp;&nbsp;&nbsp;#quot;baz#quot;: 30,
            }")
            input --> db --> output
            style output text-align:left
        end

        Wait -- Reach tasks limit / Timeout -->
        cb --> rt("Return value to each Request")
    end

    req1[Request 1] --> q_foo_("Query(#quot;foo#quot;)"):::bgFoo --> A
    req2[Request 2] --> q_foo2("Query(#quot;foo#quot;)"):::bgFoo --> A
    req3[Request 3] --> q_bar_("Query(#quot;bar#quot;)"):::bgBar --> A
    req4[Request 4] --> q_baz_("Query(#quot;baz#quot;)"):::bgBaz --> A
    req5[Request 5] --> q_baz2("Query(#quot;baz#quot;)"):::bgBaz --> A

    A --- rtn1("return 10"):::bgFoo --> req1_[Request 1]
    A --- rtn2("return 10"):::bgFoo --> req2_[Request 2]
    A --- rtn3("return 25"):::bgBar --> req3_[Request 3]
    A --- rtn4("return 30"):::bgBaz --> req4_[Request 4]
    A --- rtn5("return 30"):::bgBaz --> req5_[Request 5]

    classDef bgFoo fill:green;
    classDef bgBar fill:blue;
    classDef bgBaz fill:purple;
```

## Inspiration

- [API Performance Tunning Story when Goalng meet with GraphQL](https://hackmd.io/zvmgdunRR8mjAjVIMx0eDA?both) - Kane Wang
- [herryg91/gobatch](https://github.com/herryg91/gobatch)
