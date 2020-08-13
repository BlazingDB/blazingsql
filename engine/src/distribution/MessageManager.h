
namespace ral {
namespace distribution {

class MessageManager {

    public:
        MessageManager(std::size_t kernel_id, std::shared_ptr<Context> context) : kernel_id(kernel_id), context{context}{

        }

        MessageManager(){}

    void scatter();
    void send_total_partition_counts(void);
    int get_total_partition_counts();

    private:
        int32_t kernel_id;
        std::shared_ptr<Context> context;
        ral::cache::CacheMachine* input_message_cache;
        ral::cache::CacheMachine* output_message_cache;
        std::map<std::string, int> node_count; //must be thread-safe
        std::vector<std::string> messages_to_wait_for; //must be thread-safe

};

}  // namespace distribution
}  // namespace ral
