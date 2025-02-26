using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MiX.Integrate.API.Client;
using MiX.Integrate.Shared.Entities.Assets;
using MiX.Integrate.Shared.Entities.Drivers;
using MiX.Integrate.Shared.Entities.Groups;
using MiX.Integrate.Shared.Entities.LibraryEvents;
using MiX.Integrate.Shared.Entities.Positions;
using Newtonsoft.Json;

namespace MiX.Integrate.Samples.PositionStream
{
	class Program
	{
		static CancellationToken _cancelToken;

		static void Main(string[] args)
		{
			using (var taskCanceller = new CancellationTokenSource())
			{
				_cancelToken = taskCanceller.Token;
				//start the sample running in the background
				var sampleTask = Task.Run(ShowPositions, _cancelToken);
				//then wait for key press
				Console.ReadKey();
				taskCanceller.Cancel();
				try
				{
					sampleTask.Wait();
				}
				catch (AggregateException ae)
				{
					foreach (var e in ae.InnerExceptions)
						Console.WriteLine("{0}: {1}", e.GetType().Name, e.Message);
				}
			}
		}

		private static async Task ShowPositions()
		{
			await SavePositions().ConfigureAwait(false);

			return;
		}

		private static async Task SavePositions()
		{
			try
			{
				string getSinceToken = "20241031000000000";

				// Retrieve base URI from configuration file:
				var apiBaseUrl = ConfigurationManager.AppSettings["ApiUrl"];
				Console.WriteLine($"Connecting to: {apiBaseUrl}");

				// Retrieve security settings from configuration file:
				var idServerResourceOwnerClientSettings = new IdServerResourceOwnerClientSettings()
				{
					BaseAddress = ConfigurationManager.AppSettings["IdentityServerBaseAddress"],
					ClientId = ConfigurationManager.AppSettings["IdentityServerClientId"],
					ClientSecret = ConfigurationManager.AppSettings["IdentityServerClientSecret"],
					UserName = ConfigurationManager.AppSettings["IdentityServerUserName"],
					Password = ConfigurationManager.AppSettings["IdentityServerPassword"],
					Scopes = ConfigurationManager.AppSettings["IdentityServerScopes"]
				};

				// Retrieve list of groups the authenticated user has access to:
				var groups = await GetAvailableOrganisationsAsync(apiBaseUrl, idServerResourceOwnerClientSettings);
				if ((groups?.Count ?? 0) < 1)
				{
					Console.WriteLine("");
					Console.WriteLine("=======================================================================");
					Console.WriteLine("No available organisations found - terminating.");
					return;
				}
				
				// the rest of this sample will only process using the first available organisation
				var group = groups[1];// 0];

				List<Asset> assets = await SaveAssets(apiBaseUrl, idServerResourceOwnerClientSettings, group);

				await SaveDrivers(apiBaseUrl, idServerResourceOwnerClientSettings, group, assets);
				await SaveEventsLibruary(apiBaseUrl, idServerResourceOwnerClientSettings, group, assets);

				//
				// For this sample code the start point will be 1 hour before the sample
				// is executed. In a production service this sould only be seeded on
				// first execution and persisted between executions so that the stream
				// is read correctly

				getSinceToken = "20241116000000000";
				getSinceToken = await SaveTrips(apiBaseUrl, idServerResourceOwnerClientSettings, group, getSinceToken).ConfigureAwait(false);

				//getSinceToken = "20241112125317000";
				//getSinceToken = await SaveEvents(apiBaseUrl, idServerResourceOwnerClientSettings, group, getSinceToken).ConfigureAwait(false);

				//getSinceToken = "20241113103718000";
				//getSinceToken = await SavePositions(apiBaseUrl, idServerResourceOwnerClientSettings, group, getSinceToken).ConfigureAwait(false);

			}
			catch (Exception ex)
			{
				Console.WriteLine("");
				Console.WriteLine("=======================================================================");
				Console.WriteLine("Unexpected error:");
				PrintException(ex);
			}
			finally
			{
				Console.WriteLine("");
				Console.WriteLine("=======================================================================");
				Console.WriteLine("");
			}
		}

		private static async Task<string> SaveEvents(string apiBaseUrl, IdServerResourceOwnerClientSettings idServerResourceOwnerClientSettings, Group group, string getSinceToken)
		{
			//
			// Setup helper client and go into process loop
			var eventsClient = new EventsClient(apiBaseUrl, idServerResourceOwnerClientSettings);

			var groupIds = new List<long> { group.GroupId };
			do
			{
				Console.WriteLine("");
				Console.WriteLine("=======================================================================");
				Console.WriteLine("Requesting events....");

				var haveMoreItems = false;
				do
				{
					await Task.Delay(3000, _cancelToken); //wait 3 seconds

					var requestResult = await eventsClient.GetCreatedSinceForGroupsAsync(groupIds, "Driver", getSinceToken, 1000).ConfigureAwait(false);

					haveMoreItems = requestResult.HasMoreItems;
					var events = requestResult.Items;

					// Parse the datetime string into a DateTime object using the exact format
					DateTime datetime;
					if (DateTime.TryParseExact(getSinceToken, "yyyyMMddHHmmssfff", CultureInfo.InvariantCulture, DateTimeStyles.None, out datetime))
					{
						Console.WriteLine($"SinceToken: {datetime.ToString("yyyy-MM-dd, HH:mm ddd")}  Retrieved {events.Count} events. ");
					}

					//ProcessPositions(positions, assets);
					var jsonData = JsonConvert.SerializeObject(events, Formatting.Indented);

					// Generate a timestamped filename
					var filePath = $"c:\\mix\\9056302056092335278\\events\\events_{getSinceToken}.json";

					// Write the JSON string to the file
					SaveFile(jsonData, filePath);

					// persist token for next retrieval.
					getSinceToken = requestResult.GetSinceToken;
				} while (haveMoreItems);

				//
				// pause to prevent excessive calls to API.
				Console.WriteLine("wait 30 seconds");
				await Task.Delay(30000, _cancelToken); //wait 30 seconds

			} while (!_cancelToken.IsCancellationRequested);
			return getSinceToken;
		}


		private static async Task<string> SaveTrips(string apiBaseUrl, IdServerResourceOwnerClientSettings idServerResourceOwnerClientSettings, Group group, string getSinceToken)
		{
			//
			// Setup helper client and go into process loop
			var tripsClient = new TripsClient(apiBaseUrl, idServerResourceOwnerClientSettings);

			var groupIds = new List<long> { group.GroupId };
			do
			{
				Console.WriteLine("");
				Console.WriteLine("=======================================================================");
				Console.WriteLine("Requesting trips....");

				var haveMoreItems = false;
				do
				{
					await Task.Delay(3000, _cancelToken); //wait 3 seconds

					var requestResult = await tripsClient.GetCreatedSinceForGroupsAsync(groupIds, "Asset", getSinceToken, 1000).ConfigureAwait(false);

					haveMoreItems = requestResult.HasMoreItems;
					var trips = requestResult.Items;

					// Parse the datetime string into a DateTime object using the exact format
					DateTime datetime;
					if (DateTime.TryParseExact(getSinceToken, "yyyyMMddHHmmssfff", CultureInfo.InvariantCulture, DateTimeStyles.None, out datetime))
					{
						Console.WriteLine($"SinceToken: {datetime.ToString("yyyy-MM-dd, HH:mm ddd")}  Retrieved {trips.Count} trips. ");
					}

					//ProcessPositions(positions, assets);
					var jsonData = JsonConvert.SerializeObject(trips, Formatting.Indented);

					// Generate a timestamped filename
					var filePath = $"c:\\mix\\9056302056092335278\\trips\\trips_{getSinceToken}.json";

					// Write the JSON string to the file
					SaveFile(jsonData, filePath);

					// persist token for next retrieval.
					getSinceToken = requestResult.GetSinceToken;
				} while (haveMoreItems);

				//
				// pause to prevent excessive calls to API.
				Console.WriteLine("wait 30 seconds");
				await Task.Delay(30000, _cancelToken); //wait 30 seconds

			} while (!_cancelToken.IsCancellationRequested);
			return getSinceToken;
		}


		private static async Task<string> SavePositions(string apiBaseUrl, IdServerResourceOwnerClientSettings idServerResourceOwnerClientSettings, Group group, string getSinceToken)
		{
			//
			// Setup helper client and go into process loop
			var positionClient = new PositionsClient(apiBaseUrl, idServerResourceOwnerClientSettings);
			var groupIds = new List<long> { group.GroupId };
			do
			{
				Console.WriteLine("");
				Console.WriteLine("=======================================================================");
				Console.WriteLine("Requesting positions....");

				var haveMoreItems = false;
				do
				{
					await Task.Delay(3000, _cancelToken); //wait 3 seconds

					var requestResult = await positionClient.GetCreatedSinceForGroupsAsync(groupIds, "Asset", getSinceToken, 1000).ConfigureAwait(false);

					haveMoreItems = requestResult.HasMoreItems;
					var positions = requestResult.Items;

					// Parse the datetime string into a DateTime object using the exact format
					DateTime datetime;
					if (DateTime.TryParseExact(getSinceToken, "yyyyMMddHHmmssfff", CultureInfo.InvariantCulture, DateTimeStyles.None, out datetime))
					{
						Console.WriteLine($"SinceToken: {datetime.ToString("yyyy-MM-dd, HH:mm ddd")}  Retrieved {positions.Count} positions. ");
					}

					//ProcessPositions(positions, assets);
					var jsonData = JsonConvert.SerializeObject(positions, Formatting.Indented);

					// Generate a timestamped filename
					var filePath = $"c:\\mix\\9056302056092335278\\positions\\positions_{getSinceToken}.json";

					// Write the JSON string to the file
					SaveFile(jsonData, filePath);

					// persist token for next retrieval.
					getSinceToken = requestResult.GetSinceToken;
				} while (haveMoreItems);

				//
				// pause to prevent excessive calls to API.
				Console.WriteLine("wait 30 seconds");
				await Task.Delay(30000, _cancelToken); //wait 30 seconds

			} while (!_cancelToken.IsCancellationRequested);
			return getSinceToken;
		}

		private static async Task SaveEventsLibruary(string apiBaseUrl, IdServerResourceOwnerClientSettings idServerResourceOwnerClientSettings, Group group, List<Asset> assets)
		{
			// Retrieve a list of libraryEvents available in the organisation
			var libraryEvents = await GetLibraryEventsAsync(group.GroupId, apiBaseUrl, idServerResourceOwnerClientSettings);
			if (libraryEvents.Count > 0)
			{
				var jsonData = JsonConvert.SerializeObject(libraryEvents, Formatting.Indented);
				var filePath = $"c:\\mix\\9056302056092335278\\eventslibrary\\eventslibrary.json";
				SaveFile(jsonData, filePath);

				Console.WriteLine("");
				Console.WriteLine("=======================================================================");
				Console.WriteLine($"{libraryEvents.Count} libraryEvents found for {group.Name}.");
				Console.WriteLine($"{libraryEvents.Count} libraryEvents saved to file {filePath}.");
			}
		}

		private static async Task SaveDrivers(string apiBaseUrl, IdServerResourceOwnerClientSettings idServerResourceOwnerClientSettings, Group group, List<Asset> assets)
		{
			// Retrieve a list of drivers available in the organisation
			var drivers = await GetDriversAsync(group.GroupId, apiBaseUrl, idServerResourceOwnerClientSettings);
			if (drivers.Count > 0)
			{
				var jsonData = JsonConvert.SerializeObject(drivers, Formatting.Indented);
				var filePath = $"c:\\mix\\9056302056092335278\\drivers\\drivers.json";
				SaveFile(jsonData, filePath);

				Console.WriteLine("");
				Console.WriteLine("=======================================================================");
				Console.WriteLine($"{drivers.Count} drivers found for {group.Name}.");
				Console.WriteLine($"{drivers.Count} drivers saved to file {filePath}.");
			}
		}

		private static async Task<List<Asset>> SaveAssets(string apiBaseUrl, IdServerResourceOwnerClientSettings idServerResourceOwnerClientSettings, Group group)
		{
			// Retrieve a list of Assets available in the organisation
			var assets = await GetAssetsAsync(group.GroupId, apiBaseUrl, idServerResourceOwnerClientSettings);
			if (assets.Count > 0)
			{
				var jsonData = JsonConvert.SerializeObject(assets, Formatting.Indented);
				var filePath = $"c:\\mix\\9056302056092335278\\assets\\assets.json";
				SaveFile(jsonData, filePath);

				Console.WriteLine("");
				Console.WriteLine("=======================================================================");
				Console.WriteLine($"{assets.Count} assets found for {group.Name}.");
				Console.WriteLine($"{assets.Count} assets saved to file {filePath}.");
			}

			return assets;
		}

		private static void SaveFile(string jsonData, string fullPath)
		{
			File.WriteAllText(fullPath, jsonData);
		}

		private static async Task<List<Group>> GetAvailableOrganisationsAsync(string apiBaseUrl, IdServerResourceOwnerClientSettings idServerResourceOwnerClientSettings)
		{
			Console.WriteLine("Retrieving Organisation details");
			var groupsClient = new GroupsClient(apiBaseUrl, idServerResourceOwnerClientSettings);
			var groups = await groupsClient.GetAvailableOrganisationsAsync();
			return groups;
		}

		private static async Task<List<Asset>> GetAssetsAsync(long groupId, string apiBaseUrl, IdServerResourceOwnerClientSettings idServerResourceOwnerClientSettings)
		{
			Console.WriteLine("Retrieving Asset list...");
			var assetsClient = new AssetsClient(apiBaseUrl, idServerResourceOwnerClientSettings);
			var assets = await assetsClient.GetAllAsync(groupId);
			return assets;
		}

		private static async Task<List<Driver>> GetDriversAsync(long groupId, string apiBaseUrl, IdServerResourceOwnerClientSettings idServerResourceOwnerClientSettings)
		{
			Console.WriteLine("Retrieving Driver list...");
			var driversClient = new DriversClient(apiBaseUrl, idServerResourceOwnerClientSettings);
			var drivers = await driversClient.GetAllDriversAsync(groupId);
			return drivers;
		}
		private static async Task<IList<LibraryEvent>> GetLibraryEventsAsync(long groupId, string apiBaseUrl, IdServerResourceOwnerClientSettings idServerResourceOwnerClientSettings)
		{
			Console.WriteLine("Retrieving Driver list...");
			var libraryEventsClient = new LibraryEventsClient(apiBaseUrl, idServerResourceOwnerClientSettings);
			var libraryEvents = await libraryEventsClient.GetAllLibraryEventsAsync(groupId);
			return libraryEvents;
		}

		private static void ProcessPositions(List<Position> positions, List<Asset> assets)
		{

			//Sample just prints positions to console window.

			// Join postion and asset lists to be able to print Asset registration number and position.
			foreach (var item in from pos in positions
													 join ast in assets on pos.AssetId equals ast.AssetId
													 select new
													 {
														 Registration = ast.RegistrationNumber,
														 Timestamp = pos.Timestamp,
														 Latitude = pos.Latitude,
														 Longitutde = pos.Longitude,
														 Odometer = pos.OdometerKilometres
													 })
			{
				Console.WriteLine($"{item.Registration,-15}  {item.Timestamp}  {item.Latitude:N6} - {item.Longitutde:N6}  {item.Odometer,10:N1} Km");
			}
		}

		private static void PrintException(Exception ex)
		{
			Console.WriteLine(ex.Message);
			if (ex.InnerException != null) PrintException(ex.InnerException);
		}
	}
}
