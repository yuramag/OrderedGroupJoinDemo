using System;
using System.Collections.Generic;
using System.Linq;

namespace OrderedGroupJoinDemo
{
    public static class EnumerableExtensions
    {
        private enum JoinType
        {
            Inner,
            Left,
            Right,
            Full
        }

        private class FilteredEnumerator<T> : IDisposable
        {
            private bool m_hasData;
            private readonly IEnumerator<T> m_enumerator;

            public FilteredEnumerator(IEnumerable<T> sequence)
            {
                m_enumerator = sequence.GetEnumerator();
                m_hasData = m_enumerator.MoveNext();
            }

            public IEnumerable<T> SkipAndTakeWhile(Predicate<T> filter)
            {
                while (m_hasData && !filter(m_enumerator.Current))
                    m_hasData = m_enumerator.MoveNext();

                while (m_hasData && filter(m_enumerator.Current))
                {
                    yield return m_enumerator.Current;
                    m_hasData = m_enumerator.MoveNext();
                }
            }

            public IEnumerable<T> SkipAndTakeWhile(Func<T, int> comparer)
            {
                while (m_hasData && comparer(m_enumerator.Current) > 0)
                    m_hasData = m_enumerator.MoveNext();

                while (m_hasData && comparer(m_enumerator.Current) == 0)
                {
                    yield return m_enumerator.Current;
                    m_hasData = m_enumerator.MoveNext();
                }
            }

            public void Dispose()
            {
                m_enumerator.Dispose();
            }
        }

        private static IEnumerable<TResult> OrderedJoinIterator<TOuter, TInner, TKey, TResult>(
            this IEnumerable<TOuter> outer, IEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector,
            Func<TOuter, TInner, TResult> resultSelector, JoinType jt, IComparer<TKey> comparer)
        {
            if (outer == null) throw new ArgumentNullException("outer");
            if (inner == null) throw new ArgumentNullException("inner");
            if (outerKeySelector == null) throw new ArgumentNullException("outerKeySelector");
            if (innerKeySelector == null) throw new ArgumentNullException("innerKeySelector");
            if (resultSelector == null) throw new ArgumentNullException("resultSelector");

            if (comparer == null)
                comparer = Comparer<TKey>.Default;

            var l = outer.GetEnumerator();
            var r = inner.GetEnumerator();

            var lHasData = l.MoveNext();
            var rHasData = r.MoveNext();

            while (lHasData || rHasData)
            {
                if (!lHasData)
                {
                    if (jt == JoinType.Inner || jt == JoinType.Left)
                        yield break;
                    yield return resultSelector(default(TOuter), r.Current);
                    rHasData = r.MoveNext();
                    continue;
                }

                if (!rHasData)
                {
                    if (jt == JoinType.Inner || jt == JoinType.Right)
                        yield break;
                    yield return resultSelector(l.Current, default(TInner));
                    lHasData = l.MoveNext();
                    continue;
                }

                var comp = comparer.Compare(outerKeySelector(l.Current), innerKeySelector(r.Current));

                if (comp < 0)
                {
                    if (jt == JoinType.Left || jt == JoinType.Full)
                        yield return resultSelector(l.Current, default(TInner));
                    lHasData = l.MoveNext();
                }
                else if (comp > 0)
                {
                    if (jt == JoinType.Right || jt == JoinType.Full)
                        yield return resultSelector(default(TOuter), r.Current);
                    rHasData = r.MoveNext();
                }
                else
                {
                    yield return resultSelector(l.Current, r.Current);
                    lHasData = l.MoveNext();
                    rHasData = r.MoveNext();
                }
            }
        }

        public static IEnumerable<TResult> OrderedInnerJoin<TOuter, TInner, TKey, TResult>(
            this IEnumerable<TOuter> left, IEnumerable<TInner> right, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector,
            Func<TOuter, TInner, TResult> resultSelector, IComparer<TKey> comparer = null)
        {
            return OrderedJoinIterator(left, right, outerKeySelector, innerKeySelector, resultSelector, JoinType.Inner, comparer);
        }

        public static IEnumerable<TResult> OrderedFullJoin<TOuter, TInner, TKey, TResult>(
            this IEnumerable<TOuter> left, IEnumerable<TInner> right, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector,
            Func<TOuter, TInner, TResult> resultSelector, IComparer<TKey> comparer = null)
        {
            return OrderedJoinIterator(left, right, outerKeySelector, innerKeySelector, resultSelector, JoinType.Full, comparer);
        }

        public static IEnumerable<TResult> OrderedLeftJoin<TOuter, TInner, TKey, TResult>(
            this IEnumerable<TOuter> left, IEnumerable<TInner> right, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector,
            Func<TOuter, TInner, TResult> resultSelector, IComparer<TKey> comparer = null)
        {
            return OrderedJoinIterator(left, right, outerKeySelector, innerKeySelector, resultSelector, JoinType.Left, comparer);
        }

        public static IEnumerable<TResult> OrderedRightJoin<TOuter, TInner, TKey, TResult>(
            this IEnumerable<TOuter> left, IEnumerable<TInner> right, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector,
            Func<TOuter, TInner, TResult> resultSelector, IComparer<TKey> comparer = null)
        {
            return OrderedJoinIterator(left, right, outerKeySelector, innerKeySelector, resultSelector, JoinType.Right, comparer);
        }

        public static IEnumerable<TResult> OrderedEqualityGroupJoin<TOuter, TInner, TKey, TResult>(this IEnumerable<TOuter> outer,
            IEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector,
            Func<TOuter, IEnumerable<TInner>, TResult> resultSelector, IEqualityComparer<TKey> comparer = null)
        {
            if (outer == null) throw new ArgumentNullException("outer");
            if (inner == null) throw new ArgumentNullException("inner");
            if (outerKeySelector == null) throw new ArgumentNullException("outerKeySelector");
            if (innerKeySelector == null) throw new ArgumentNullException("innerKeySelector");
            if (resultSelector == null) throw new ArgumentNullException("resultSelector");

            if (comparer == null)
                comparer = EqualityComparer<TKey>.Default;

            var innerEnumerator = new FilteredEnumerator<TInner>(inner);
            return outer.Select(outerItem => resultSelector(outerItem, innerEnumerator.SkipAndTakeWhile(innerItem => comparer.Equals(outerKeySelector(outerItem), innerKeySelector(innerItem)))));
        }

        public static IEnumerable<TResult> OrderedCompareGroupJoin<TOuter, TInner, TKey, TResult>(this IEnumerable<TOuter> outer,
            IEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector,
            Func<TOuter, IEnumerable<TInner>, TResult> resultSelector, IComparer<TKey> comparer = null)
        {
            if (outer == null) throw new ArgumentNullException("outer");
            if (inner == null) throw new ArgumentNullException("inner");
            if (outerKeySelector == null) throw new ArgumentNullException("outerKeySelector");
            if (innerKeySelector == null) throw new ArgumentNullException("innerKeySelector");
            if (resultSelector == null) throw new ArgumentNullException("resultSelector");

            if (comparer == null)
                comparer = Comparer<TKey>.Default;

            var innerEnumerator = new FilteredEnumerator<TInner>(inner);
            return outer.Select(outerItem => resultSelector(outerItem, innerEnumerator.SkipAndTakeWhile(innerItem => comparer.Compare(outerKeySelector(outerItem), innerKeySelector(innerItem)))));
        }
    }
}