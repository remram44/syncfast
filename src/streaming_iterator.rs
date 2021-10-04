pub trait StreamingIterator<'a> {
    type Item: 'a;

    fn next(&'a mut self) -> Option<Self::Item>;
}
