using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace OrderedGroupJoinDemo
{
    internal class Program
    {
        private const int c_count = 10*1000*1000;
        private const int c_skipCount = 1*1000*1000;
        private const int c_takeCount = 3;

        private enum DemoId
        {
            StandardGroupJoin,
            OrderedEqualityGroupJoin,
            OrderedCompareGroupJoin,
            StandardJoin,
            OrderedInnerJoin,
            OrderedFullJoin,
            OrderedLeftJoin,
            OrderedRightJoin
        }

        private static void Main()
        {
            var canExit = false;
            while (!canExit)
            {
                Console.WriteLine("Enter Demo ID:");
                foreach (var demoId in typeof (DemoId).GetEnumValues())
                    Console.WriteLine("\t{0}. {1}", (int) demoId + 1, demoId);
                Console.Write("\n|> ");
                int id;
                var entry = Console.ReadLine();
                if (string.IsNullOrEmpty(entry))
                    canExit = true;
                else if (int.TryParse(entry, out id))
                    RunDemo((DemoId) id - 1);
                Console.WriteLine();
            }
        }

        private static void RunDemo(DemoId demoId)
        {
            Console.WriteLine("\nRESULTS ({0}):\n", demoId);

            var sw = Stopwatch.StartNew();

            var masterData = GetMasterData(c_count);
            var detailData = GetDetailData(c_count);

            var outerData = GetMasterData(c_count).Where(x => x.MasterId != c_skipCount + 2);
            var innerData = GetMasterData(c_count).Where(x => x.MasterId != c_skipCount + 3);

            switch (demoId)
            {
                case DemoId.StandardGroupJoin:
                    masterData.GroupJoin(detailData, m => m.MasterId, d => d.MasterId, Tuple.Create)
                        .Skip(c_skipCount).Take(c_takeCount).PrintData();
                    break;
                case DemoId.OrderedEqualityGroupJoin:
                    masterData.OrderedEqualityGroupJoin(detailData, m => m.MasterId, d => d.MasterId, Tuple.Create)
                        .Skip(c_skipCount).Take(c_takeCount).PrintData();
                    break;
                case DemoId.OrderedCompareGroupJoin:
                    masterData.OrderedCompareGroupJoin(detailData, m => m.MasterId, d => d.MasterId, Tuple.Create)
                        .Skip(c_skipCount).Take(c_takeCount).PrintData();
                    break;
                case DemoId.StandardJoin:
                    outerData.Join(innerData, x => x.MasterId, x => x.MasterId, Tuple.Create)
                        .Skip(c_skipCount).Take(c_takeCount).PrintData();
                    break;
                case DemoId.OrderedInnerJoin:
                    outerData.OrderedInnerJoin(innerData, x => x.MasterId, x => x.MasterId, Tuple.Create)
                        .Skip(c_skipCount).Take(c_takeCount).PrintData();
                    break;
                case DemoId.OrderedFullJoin:
                    outerData.OrderedFullJoin(innerData, x => x.MasterId, x => x.MasterId, Tuple.Create)
                        .Skip(c_skipCount).Take(c_takeCount).PrintData();
                    break;
                case DemoId.OrderedLeftJoin:
                    outerData.OrderedLeftJoin(innerData, x => x.MasterId, x => x.MasterId, Tuple.Create)
                        .Skip(c_skipCount).Take(c_takeCount).PrintData();
                    break;
                case DemoId.OrderedRightJoin:
                    outerData.OrderedRightJoin(innerData, x => x.MasterId, x => x.MasterId, Tuple.Create)
                        .Skip(c_skipCount).Take(c_takeCount).PrintData();
                    break;
            }

            Console.WriteLine("\nElapsed (ms): {0}", sw.ElapsedMilliseconds);
        }

        private static IEnumerable<MasterData> GetMasterData(int count)
        {
            return Enumerable.Range(1, count).Select(m => new MasterData {MasterId = m});
        }

        private static IEnumerable<DetailData> GetDetailData(int count)
        {
            return Enumerable.Range(1, count).SelectMany(m => Enumerable.Range(1, 5).Select(d => new DetailData {MasterId = m, DetailId = d}));
        }
    }

    public static class PrintingExtensions
    {
        public static void PrintData(this IEnumerable<Tuple<MasterData, MasterData>> data)
        {
            foreach (var masterItem in data)
                Console.WriteLine("{0} <===> {1}", masterItem.Item1.Data, masterItem.Item2.Data);
        }

        public static void PrintData(this IEnumerable<Tuple<MasterData, IEnumerable<DetailData>>> data)
        {
            foreach (var masterItem in data)
            {
                Console.WriteLine(masterItem.Item1.Data);
                foreach (var detailItem in masterItem.Item2)
                    Console.WriteLine("\t{0}", detailItem.Data);
            }
        } 
    }

    public struct MasterData
    {
        public int MasterId { get; set; }

        public string Data
        {
            get { return string.Format("MASTER(Master ID: {0})", MasterId); }
        }
    }

    public struct DetailData
    {
        public int MasterId { get; set; }
        public int DetailId { get; set; }

        public string Data
        {
            get { return string.Format("DETAIL(Master ID: {0}, Detail ID: {1})", MasterId, DetailId); }
        }
    }
}