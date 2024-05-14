#include <iostream>

#include <rclcpp/rclcpp.hpp>
#include <rclcpp/serialization.hpp>
#include <example_interfaces/msg/int32.hpp>

#include <rosbag2_cpp/writer.hpp>
#include <rosbag2_cpp/writers/sequential_writer.hpp>
#include <rosbag2_storage/serialized_bag_message.hpp>

#include <geometry_msgs/msg/pose.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <builtin_interfaces/msg/time.hpp>
#include <builtin_interfaces/msg/detail/duration__struct.hpp>

#include <rosbag2_cpp/writer.hpp>
#include <rosbag2_cpp/writers/sequential_writer.hpp>
#include <rosbag2_storage/serialized_bag_message.hpp>
#include <fstream>

using geometry_msgs::msg::PoseStamped;
using std::placeholders::_1;
using namespace std;

int main(int argc, char **argv)
{

  string csvfile;

  if (argc >= 2)
  {
    csvfile = argv[1];
    cout << "csvfile = " << csvfile << endl;
  }
  else
  {
    cout << "USAGE: genbag csvfile" << endl;
    return 0;
  }

  PoseStamped data;
  ifstream fin;

  const rosbag2_cpp::StorageOptions storage_options({csvfile, "sqlite3"});
  const rosbag2_cpp::ConverterOptions converter_options(
      {rmw_get_serialization_format(),
       rmw_get_serialization_format()});

  std::unique_ptr<rosbag2_cpp::writers::SequentialWriter> writer_ =
      std::make_unique<rosbag2_cpp::writers::SequentialWriter>();

  writer_->open(storage_options, converter_options);

  writer_->create_topic(
      {csvfile,
       "geometry_msgs/msg/PoseStamped",
       rmw_get_serialization_format(),
       ""});

  rcutils_time_point_value_t time_stamp;

  if (rcutils_system_time_now(&time_stamp) != RCUTILS_RET_OK)
  {
    std::cerr << "Error getting current time: " << rcutils_get_error_string().str;
    return 1;
  }

  fin.open(csvfile + ".csv", ios::in);

  vector<string> row;
  string temp, line, word;

  while (!fin.eof())
  {

    row.clear();
    getline(fin, line);
    stringstream s(line);

    if (line[0] == '%' || line == "")
      continue;

    while (getline(s, word, ','))
    {
      row.push_back(word);
    }

    time_stamp = (long)(stold(row[0]) * 100000000000000);

    int32_t sec = time_stamp / 1000000000;
    uint32_t nanosec = time_stamp % 1000000000;

    data.header.frame_id = row[3];
    data.header.stamp.sec = sec;
    data.header.stamp.nanosec = nanosec;

    data.pose.position.x = stod(row[5]);
    data.pose.position.y = stod(row[6]);
    data.pose.position.z = stod(row[7]);
    data.pose.orientation.x = stod(row[8]);
    data.pose.orientation.y = stod(row[9]);
    data.pose.orientation.z = stod(row[10]);
    data.pose.orientation.w = stod(row[11]);

    auto serializer = rclcpp::Serialization<geometry_msgs::msg::Pose>();
    auto serialized_message = rclcpp::SerializedMessage();
    serializer.serialize_message(&data, &serialized_message);

    auto bag_message = std::make_shared<rosbag2_storage::SerializedBagMessage>();

    bag_message->serialized_data = std::shared_ptr<rcutils_uint8_array_t>(
        new rcutils_uint8_array_t,
        [](rcutils_uint8_array_t *msg)
        {
          auto fini_return = rcutils_uint8_array_fini(msg);
          delete msg;
          if (fini_return != RCUTILS_RET_OK)
          {
            std::cerr << "Failed to destroy serialized message " << rcutils_get_error_string().str;
          }
        });

    *bag_message->serialized_data = serialized_message.release_rcl_serialized_message();

    bag_message->topic_name = csvfile;
    bag_message->time_stamp = time_stamp;

    writer_->write(bag_message);
  }
  return 0;
}