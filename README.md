<h1>C# LINQ: Ordered Joining and Grouping Lazy Operators</h1>

<h2>Introduction</h2>

<p>This article introduces one of possible implementations of <code>Join </code>and <code>GroupJoin </code>LINQ extensions assuming that the source data is ordered (pre-sorted). Exploiting this assumption, let us build Joining and Grouping logic using lazy evaluation as opposed to standard LINQ <code>Join </code>and <code>GroupJoin </code>operators that require the whole sequence to be preloaded into memory.</p>

<p>Although C# is used for this article, the technique applies to all .NET languages.</p>

<h2>Background</h2>

<p>Let&rsquo;s say we have two datasets &ndash; <code>Master </code>and <code>Detail</code>. Both of them contain lots of records ordered by Master ID field and have master/detail relationship. The datasets are not necessarily stored in database, they could be anywhere (in our example, we will generate fake data sequences on the fly). Now the task is to read data from both sequences, join results together by Master ID, and perform some operations on resulting sequence (just print in our example). Of course, if data was stored in database, we could have written a SQL JOIN statement and let SQL Server perform joining. But for the sake of demonstration, let&rsquo;s try implementing joining and grouping ourselves, right in the C# code.</p>

<h2>Data Structures</h2>

<p>In our demo, we will use the following data structures called <code>MasterData </code>and <code>DetailData</code>:</p>

<pre lang="cs">
public struct MasterData
{
    public int MasterId { get; set; }
 
    public string Data
    {
        get { return string.Format(&quot;MASTER(Master ID: {0})&quot;, MasterId); }
    }
}
 
public struct DetailData
{
    public int MasterId { get; set; }
    public int DetailId { get; set; }
 
    public string Data
    {
        get { return string.Format(&quot;DETAIL(Master ID: {0}, Detail ID: {1})&quot;, MasterId, DetailId); }
    }
}</pre>

<p>We will also use <code>PrintData </code>helper extension methods to print data:</p>

<pre lang="cs">
public static class PrintingExtensions
{
    public static void PrintData(this IEnumerable&lt;Tuple&lt;MasterData, MasterData&gt;&gt; data)
    {
        foreach (var masterItem in data)
            Console.WriteLine(&quot;{0} &lt;===&gt; {1}&quot;, masterItem.Item1.Data, masterItem.Item2.Data);
    }
 
    public static void PrintData(this IEnumerable&lt;Tuple&lt;MasterData, IEnumerable&lt;DetailData&gt;&gt;&gt; data)
    {
        foreach (var masterItem in data)
        {
            Console.WriteLine(masterItem.Item1.Data);
            foreach (var detailItem in masterItem.Item2)
                Console.WriteLine(&quot;\t{0}&quot;, detailItem.Data);
        }
    } 
}</pre>

<p>And finally two methods to generate sequences of sample data:</p>

<pre lang="cs">
private static IEnumerable&lt;MasterData&gt; GetMasterData(int count)
{
    return Enumerable.Range(1, count).Select(m =&gt; new MasterData {MasterId = m});
}
 
private static IEnumerable&lt;DetailData&gt; GetDetailData(int count)
{
    return Enumerable.Range(1, count).SelectMany(m =&gt; 
    Enumerable.Range(1, 5).Select(d =&gt; new DetailData {MasterId = m, DetailId = d}));
}</pre>

<h2>GroupJoin - Standard Approach</h2>

<p>Here is how we could do it using standard <code>GroupJoin </code>LINQ operator:</p>

<pre lang="cs">
private const int c_count = 10*1000*1000;
private const int c_skipCount = 1*1000*1000;
private const int c_takeCount = 3;
 
void Main()
{    
    var masterData = GetMasterData(c_count);
    var detailData = GetDetailData(c_count);
    
    masterData.GroupJoin(detailData, m =&gt; m.MasterId, d =&gt; d.MasterId, Tuple.Create)
        .Skip(c_skipCount).Take(c_takeCount).PrintData();
}</pre>

<p>This works perfectly well with relatively short sequences, but when the number of items is big, like in this example, execution becomes not very optimal because standard LINQ <code>GroupJoin </code>operator requires the entire sequences to be loaded into memory first, so that joining and grouping can work correctly for even un-ordered sequences.</p>

<p>However, according to our initial requirements, both sequences should be sorted by Master ID. If that&rsquo;s true, why read the entire sequence if we can achieve the same result in a lazy manner? Let&rsquo;s see how it might look like.</p>

<h2>GroupJoin - Optimized Approach</h2>

<p>In order to exploit the fact that the datasets are ordered, I have created a few extension methods on <code>IEnumerable&lt;T&gt;</code>. Here is an example that gives results identical to the previous one, though faster and more optimal:</p>

<pre lang="cs">
private const int c_count = 10*1000*1000;
private const int c_skipCount = 1*1000*1000;
private const int c_takeCount = 3;
 
void Main()
{    
    var masterData = GetMasterData(c_count);
    var detailData = GetDetailData(c_count);
    
    masterData.OrderedEqualityGroupJoin(detailData, m =&gt; m.MasterId, d =&gt; d.MasterId, Tuple.Create)
        .Skip(c_skipCount).Take(c_takeCount).PrintData();
}</pre>

<p><code>OrderedEqualityGroupJoin </code>operator, as well as <code>OrderedCompareGroupJoin</code>, internally uses the following class to achieve lazy evaluation:</p>

<pre lang="cs">
private class FilteredEnumerator&lt;T&gt; : IDisposable
{
    private bool m_hasData;
    private readonly IEnumerator&lt;T&gt; m_enumerator;
 
    public FilteredEnumerator(IEnumerable&lt;T&gt; sequence)
    {
        m_enumerator = sequence.GetEnumerator();
        m_hasData = m_enumerator.MoveNext();
    }
 
    public IEnumerable&lt;T&gt; SkipAndTakeWhile(Predicate&lt;T&gt; filter)
    {
        while (m_hasData &amp;&amp; !filter(m_enumerator.Current))
            m_hasData = m_enumerator.MoveNext();
 
        while (m_hasData &amp;&amp; filter(m_enumerator.Current))
        {
            yield return m_enumerator.Current;
            m_hasData = m_enumerator.MoveNext();
        }
    }
 
    public IEnumerable&lt;T&gt; SkipAndTakeWhile(Func&lt;T, int&gt; comparer)
    {
        while (m_hasData &amp;&amp; comparer(m_enumerator.Current) &gt; 0)
            m_hasData = m_enumerator.MoveNext();
 
        while (m_hasData &amp;&amp; comparer(m_enumerator.Current) == 0)
        {
            yield return m_enumerator.Current;
            m_hasData = m_enumerator.MoveNext();
        }
    }
 
    public void Dispose()
    {
        m_enumerator.Dispose();
    }
}</pre>

<p><code>FilteredEnumerator </code>will skip all <code>Detail </code>records until it finds the one that matches current Master key, and will only pick up those that match. The difference between the two is that <code>OrderedEqualityGroupJoin </code>is based on equality of keys, whereas <code>OrderedCompareGroupJoin </code>is based on comparing keys. This becomes relevant when some <code>Master </code>records exist that do not have corresponding <code>Detail </code>records. Operator based on equality of keys might skip the entire sequence in that case, but the one based on comparison of keys would only skip detail records with keys less than the current Master ID.</p>

<h2>Inner and Outer Joins</h2>

<p>So far, we talked about Group Joining and it&rsquo;s time to look into <code>Join </code>operators now. The attached source code contains implementation of Inner, Outer Full, Left, and Right joins, again, assuming that both inner and outer sequences are ordered.</p>

<p>Implementation is based on <code>OrderedJoinIterator</code>, which basically walks through both sequences and depending on comparison of keys and joining type yield returns either inner or outer records, or their default values.</p>

<pre lang="cs">
static IEnumerable&lt;TResult&gt; OrderedJoinIterator&lt;TOuter, TInner, TKey, TResult&gt;(
    this IEnumerable&lt;TOuter&gt; outer, 
    IEnumerable&lt;TInner&gt; inner, 
    Func&lt;TOuter, TKey&gt; outerKeySelector, 
    Func&lt;TInner, TKey&gt; innerKeySelector,
    Func&lt;TOuter, TInner, TResult&gt; resultSelector, 
    JoinType jt, IComparer&lt;TKey&gt; comparer)
{
    if (comparer == null)
        comparer = Comparer&lt;TKey&gt;.Default;
 
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
 
        if (comp &lt; 0)
        {
            if (jt == JoinType.Left || jt == JoinType.Full)
                yield return resultSelector(l.Current, default(TInner));
            lHasData = l.MoveNext();
        }
        else if (comp &gt; 0)
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
}</pre>

<p>Signatures and behavior of presented <code>OrderedJoin </code>operators are identical to the standard LINQ <code>Join </code>operator. Below is the list of those operators:</p>

<ul>
	<li><code>OrderedInnerJoin</code>: Returns inner and outer records that have matching keys.</li>
	<li><code>OrderedFullJoin</code>: Returns all inner and outer records. If keys are not matching on either side, type&rsquo;s default value will be returned.</li>
	<li><code>OrderedLeftJoin</code>: Returns all outer records plus either matching inner records or their type&rsquo;s defaults.</li>
	<li><code>OrderedRightJoin</code>: Returns all inner records plus either matching outer records or their type&rsquo;s defaults.</li>
</ul>

<h2>Conclusion</h2>

<p>LINQ is a very powerful technology and allows quite easily to achieve desired functionality hiding added complexity away from the main code that uses it. Although behavior of standard joining and grouping operators in LINQ-to-Objects is not targeting ordered sequences, the architecture of extension methods, lambdas, and the concept of yield returning <code>IEnumerable&lt;T&gt;</code> elements, play together very nicely and allow creating new operators with desired functionality.</p>
