use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::fs::File;

use timely::progress::Timestamp;
use timely::order::Product;
use timely::dataflow::Scope;

use differential_dataflow::VecCollection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::iterate::VecVariable;

type Node = usize;
type Edge = (Node, Node);
type Iter = usize;
type Diff = isize;

/// A direction we might traverse an edge.
#[derive(Debug)]
pub enum Relation {
    /// The forward direction of the relation, A(x,y).
    Forward(String),
    /// The reverse direction of the relation, A(y,x).
    Reverse(String),
}

/// A rule for producing edges based on paths.
///
/// The intent is that we should add to the left hand relation any pair `(x,y)` for which
/// there exists a path x -> z0 -> z1 -> .. -> y through the specified relations.
#[derive(Debug)]
pub struct Production {
    /// The name of the edge set to populate.
    pub left_hand: String,
    /// A sequence of edges to follow.
    pub relations: Vec<Relation>,
}

impl<'a> From<&'a str> for Production {
    fn from(text: &'a str) -> Production {

        let mut names = text.split_whitespace();

        // Extract the target.
        let left_hand = names.next().expect("All rules must have a target.").to_string();

        // Collect all remaining names.
        let relations = names.map(|name| {
            if name.starts_with('~') { Relation::Reverse(name.split_at(1).1.to_string()) }
            else                     { Relation::Forward(name.to_string()) }
        }).collect();

        Production { left_hand, relations }
    }
}

#[derive(Debug)]
pub struct Query {
    /// A sequence of productions.
    ///
    /// Empty productions are a great way to define input relations, but are otherwise ignored.
    pub productions: Vec<Production>,
}

use differential_dataflow::trace::implementations::{ValSpine, KeySpine};
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};

type TraceKeyHandle<K,T,R> = TraceAgent<KeySpine<K, T, R>>;
type TraceValHandle<K,V,T,R> = TraceAgent<ValSpine<K, V, T, R>>;
type Arrange<'scope, T,K,V,R> = Arranged<'scope, TraceValHandle<K, V, T, R>>;

/// An evolving set of edges.
///
/// An edge variable represents the distinct set of edges found in any of a set of productions
/// which the variable. A newly created edge variable supports the addition of new productions
/// up until the variable is "completed" at which point the definition is locked in.
///
/// An edge variable provides arranged representations of its contents, even before they are
/// completely defined, in support of recursively defined productions.
pub struct EdgeVariable<'scope, T: Timestamp + Lattice> {
    variable: VecVariable<'scope, T, Edge, Diff>,
    collection: VecCollection<'scope, T, Edge, Diff>,
    current: VecCollection<'scope, T, Edge, Diff>,
    forward: Option<Arrange<'scope, T, Node, Node, Diff>>,
    reverse: Option<Arrange<'scope, T, Node, Node, Diff>>,
}

impl<'scope, T: Timestamp + Lattice> EdgeVariable<'scope, T> {
    /// Creates a new variable initialized with `source`.
    pub fn from(source: VecCollection<'scope, T, Edge>, step: T::Summary) -> Self {
        let (variable, collection) = VecVariable::new(source.scope(), step);
        EdgeVariable {
            variable,
            collection,
            current: source.clone(),
            forward: None,
            reverse: None,
        }
    }
    /// Concatenates `production` into the definition of the variable.
    pub fn add_production(&mut self, production: VecCollection<'scope, T, Edge, Diff>) {
        self.current = self.current.clone().concat(production);
    }
    /// Finalizes the variable, connecting its recursive definition.
    ///
    /// Failure to call `complete` on a variable results in a non-recursively defined
    /// collection, whose contents are just its initial `source` data.
    pub fn complete(self) {
        let distinct = self.current.distinct();
        // distinct.map(|_| ()).consolidate().inspect(|x| println!("{:?}", x));
        self.variable.set(distinct);
    }
    /// The collection arranged in the forward direction.
    pub fn forward(&mut self) -> &Arrange<'scope, T, Node, Node, Diff> {
        if self.forward.is_none() {
            self.forward = Some(self.collection.clone().arrange_by_key());
        }
        self.forward.as_ref().unwrap()
    }
    /// The collection arranged in the reverse direction.
    pub fn reverse(&mut self) -> &Arrange<'scope, T, Node, Node, Diff> {
        if self.reverse.is_none() {
            self.reverse = Some(self.collection.clone().map(|(x,y)| (y,x)).arrange_by_key());
        }
        self.reverse.as_ref().unwrap()
    }
}

/// Handles to inputs and outputs of a computation.
pub struct RelationHandles<T: Timestamp+Lattice> {
    /// An input handle supporting arbitrary changes.
    pub input: InputSession<T, Edge, Diff>,
    /// An output trace handle which can be used in other computations.
    pub trace: TraceKeyHandle<Edge, T, Diff>,
}

impl Query {

    /// Creates a new query from a sequence of productions.
    ///
    /// Each production is a `String` of the form
    ///
    ///    Target Rel1 Rel2 ..
    ///
    /// Where each Rel may begin with a '~' character, indicating the reverse direction.
    /// Target must not begin with a '~'; rewrite the rule in the other order, silly.
    pub fn build_from<'a>(iterator: impl IntoIterator<Item=&'a str>) -> Self {
        Query { productions: iterator.into_iter().map(|text| Production::from(text)).collect() }
    }

    /// Creates a dataflow implementing the query, and returns input and trace handles.
    pub fn render_in<T>(&self, scope: Scope<T>) -> BTreeMap<String, RelationHandles<T>>
    where
        T: Timestamp + Lattice + ::timely::order::TotalOrder,
    {
        // Create new input (handle, stream) pairs
        let mut input_map = BTreeMap::new();
        for production in self.productions.iter() {
            input_map.entry(production.left_hand.clone()).or_insert_with(|| scope.new_collection());
        }

        // We need a subscope to allow iterative development of variables.
        scope.iterative::<Iter,_,_>(|subscope| {

            // create map from relation name to input handle and collection.
            let mut result_map = BTreeMap::new();
            let mut variable_map = BTreeMap::new();

            // create variables and result handles for each named relation.
            for (name, (input, collection)) in input_map {
                let edge_variable = EdgeVariable::from(collection.enter(subscope), Product::new(Default::default(), 1));
                let trace = edge_variable.collection.clone().leave(scope).arrange_by_self().trace;
                result_map.insert(name.clone(), RelationHandles { input, trace });
                variable_map.insert(name.clone(), edge_variable);
            }

            // For each rule, add to the productions for the relation.
            for production in self.productions.iter() {

                let name = &production.left_hand;
                let rule = &production.relations;

                // We need to start somewhere; ignore empty rules.
                if rule.len() > 0 {

                    // We'll track the path transposed, so that it is indexed by *destination* rather than source.
                    let mut transposed = match &rule[0] {
                        Relation::Forward(name) => variable_map.get_mut(name).unwrap().reverse().clone(),
                        Relation::Reverse(name) => variable_map.get_mut(name).unwrap().forward().clone(),
                    };

                    for relation in rule[1..].iter() {
                        let to_join = match relation {
                            Relation::Forward(name) => variable_map.get_mut(name).unwrap().forward().clone(),
                            Relation::Reverse(name) => variable_map.get_mut(name).unwrap().reverse().clone(),
                        };

                        transposed =
                        transposed
                            .join_core(to_join, |_k,&x,&y| Some((y,x)))
                            .arrange_by_key();
                    }

                    // Reverse the direction before adding it as a production.
                    variable_map.get_mut(name).unwrap().add_production(transposed.as_collection(|&dst,&src| (src,dst)));
                }
            }

            for (_name, variable) in variable_map {
                variable.complete();
            }
            result_map
        })
    }
}

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        let peers = worker.peers();
        let index = worker.index();

        let query_filename = std::env::args().nth(1).expect("Argument 1 (query filename) missing.");
        let query_text = std::fs::read_to_string(query_filename).expect("Failed to read query file");
        let query = Query::build_from(query_text.lines());

        let mut relation_map = worker.dataflow::<(),_,_>(|scope| query.render_in(scope));

        if index == 0 { println!("{:?}:\tDataflow assembled for {:?}", timer.elapsed(), query); }

        // Build a dataflow to report final sizes.
        worker.dataflow(|scope| {
            for (name, data) in relation_map.iter_mut() {
                let name = name.to_string();
                data.trace
                    .import(scope)
                    .as_collection(|&_kv,&()| ())
                    .consolidate()
                    .inspect(move |x| println!("{:?}\tfinal size of relation '{}': {:?}", timer.elapsed(), name, x.2));
            }
        });

        // snag a filename to use for the input graph.
        let data_filename = std::env::args().nth(2).expect("Argument 2 (data filename) missing.");
        let file = BufReader::new(File::open(data_filename).expect("Failed to read data file"));

        for readline in file.lines() {
            let line = readline.ok().expect("read error");
            if !line.starts_with('#') && line.len() > 0 {
                let mut elts = line[..].split_whitespace();
                let src = elts.next().expect("data line with no src (1st) element").parse().expect("malformed src");
                if (src as usize) % peers == index {
                    let dst = elts.next().expect("data line with no dst (2nd) element").parse().expect("malformed dst");
                    let val: &str = elts.next().expect("data line with no val (3rd) element");
                    if let Some(handle) = relation_map.get_mut(val) {
                        handle.input.insert((src, dst));
                    }
                    else {
                        panic!("couldn't find the named relation: {:?}", val);
                    }
                }
            }
        }

        if index == 0 { println!("{:?}:\tData loaded", timer.elapsed()); }

    }).expect("Timely computation did not complete cleanly");
}
