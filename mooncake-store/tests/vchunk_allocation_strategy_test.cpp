#include "vchunk_allocation_strategy.h"

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "allocation_strategy.h"
#include "vchunk_test_allocator.h"

namespace mooncake {
namespace {

using test::VChunkTestAllocator;

TEST(VChunkAllocationStrategyTest, AllocatesContiguousStripesAndRollsBack) {
    AllocatorManager manager;
    std::vector<std::shared_ptr<VChunkTestAllocator>> allocators;
    for (size_t i = 0; i < 3; ++i) {
        auto allocator = std::make_shared<VChunkTestAllocator>(
            "segment-" + std::to_string(i), 0x100000000ULL + i * 0x100000,
            64U * 1024U);
        manager.addAllocator(allocator->getSegmentName(), allocator);
        allocators.push_back(std::move(allocator));
    }

    {
        auto result = AllocateVChunk(manager, 9U * 4096U,
                                     VCSliceSizeLevel::k4K);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->row_size, 3U);
        ASSERT_EQ(result->allocations.size(), 9U);
        for (size_t row = 0; row < 3; ++row) {
            const auto& segment = result->allocations[row * 3].segment_name;
            for (size_t column = 0; column < 3; ++column) {
                const auto& allocation =
                    result->allocations[row * 3 + column];
                EXPECT_EQ(allocation.segment_name, segment);
                if (column > 0) {
                    EXPECT_EQ(allocation.target_offset,
                              result->allocations[row * 3 + column - 1]
                                      .target_offset +
                                  4096U);
                }
                EXPECT_EQ(allocation.slice_index, row * 3 + column);
                EXPECT_EQ(allocation.logical_length, 4096U);
                EXPECT_EQ(allocation.allocated_length, 4096U);
                EXPECT_NE(allocation.buffer, nullptr);
            }
        }
        for (const auto& allocator : allocators) {
            EXPECT_EQ(allocator->size(), 3U * 4096U);
        }
    }
    for (const auto& allocator : allocators) {
        EXPECT_EQ(allocator->size(), 0U);
    }
}

TEST(VChunkAllocationStrategyTest, TracksShortFinalLogicalSlice) {
    AllocatorManager manager;
    auto allocator = std::make_shared<VChunkTestAllocator>(
        "segment-a", 0x200000000ULL, 64U * 1024U);
    manager.addAllocator("segment-a", allocator);

    auto result =
        AllocateVChunk(manager, 10U * 1024U, VCSliceSizeLevel::k4K);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->allocations.size(), 3U);
    EXPECT_EQ(result->allocations.back().logical_length, 2048U);
    EXPECT_EQ(result->allocations.back().allocated_length, 4096U);
}

TEST(VChunkAllocationStrategyTest, HonorsExcludedSegments) {
    AllocatorManager manager;
    auto first = std::make_shared<VChunkTestAllocator>(
        "segment-a", 0x300000000ULL, 64U * 1024U);
    auto second = std::make_shared<VChunkTestAllocator>(
        "segment-b", 0x400000000ULL, 64U * 1024U);
    manager.addAllocator("segment-a", first);
    manager.addAllocator("segment-b", second);

    auto result = AllocateVChunk(manager, 8192, VCSliceSizeLevel::k4K,
                                 std::set<std::string>{"segment-a"});
    ASSERT_TRUE(result.has_value());
    for (const auto& allocation : result->allocations) {
        EXPECT_EQ(allocation.segment_name, "segment-b");
    }
    EXPECT_EQ(first->size(), 0U);
}

TEST(VChunkAllocationStrategyTest, PartialFailureRollsBackAllBuffers) {
    AllocatorManager manager;
    auto first = std::make_shared<VChunkTestAllocator>(
        "segment-a", 0x500000000ULL, 4096);
    auto second = std::make_shared<VChunkTestAllocator>(
        "segment-b", 0x600000000ULL, 4096);
    manager.addAllocator("segment-a", first);
    manager.addAllocator("segment-b", second);

    auto result =
        AllocateVChunk(manager, 3U * 4096U, VCSliceSizeLevel::k4K);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(first->size(), 0U);
    EXPECT_EQ(second->size(), 0U);
}

TEST(VChunkAllocationStrategyTest, AllocatesReplicasOnDistinctSegments) {
    AllocatorManager manager;
    std::vector<std::shared_ptr<VChunkTestAllocator>> allocators;
    for (size_t i = 0; i < 4; ++i) {
        auto allocator = std::make_shared<VChunkTestAllocator>(
            "segment-" + std::to_string(i), 0x700000000ULL + i * 0x100000,
            64U * 1024U);
        manager.addAllocator(allocator->getSegmentName(), allocator);
        allocators.push_back(std::move(allocator));
    }

    {
        auto result = AllocateVChunk(manager, 4U * 4096U,
                                     VCSliceSizeLevel::k4K, {}, 2);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->replica_num, 2U);
        ASSERT_EQ(result->allocations.size(), 8U);
        std::vector<std::unordered_set<std::string>> placements(4);
        for (const auto& allocation : result->allocations) {
            ASSERT_LT(allocation.slice_index, placements.size());
            EXPECT_TRUE(placements[allocation.slice_index]
                            .insert(allocation.segment_name)
                            .second);
            EXPECT_LT(allocation.replica_index, 2U);
        }
        for (const auto& placement : placements) {
            EXPECT_EQ(placement.size(), 2U);
        }
        EXPECT_FALSE(result->slice_groups.empty());
    }
    for (const auto& allocator : allocators) {
        EXPECT_EQ(allocator->size(), 0U);
    }
}

TEST(VChunkAllocationStrategyTest, RejectsInsufficientReplicaDomains) {
    AllocatorManager manager;
    auto allocator = std::make_shared<VChunkTestAllocator>(
        "segment-a", 0x800000000ULL, 64U * 1024U);
    manager.addAllocator("segment-a", allocator);

    auto result = AllocateVChunk(manager, 4096, VCSliceSizeLevel::k4K, {}, 2);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(allocator->size(), 0U);
}

TEST(VChunkAllocationStrategyTest, RollsBackPartialReplicaAllocation) {
    AllocatorManager manager;
    auto first = std::make_shared<VChunkTestAllocator>(
        "segment-a", 0x900000000ULL, 2U * 4096U);
    auto second = std::make_shared<VChunkTestAllocator>(
        "segment-b", 0xA00000000ULL, 4096U);
    manager.addAllocator("segment-a", first);
    manager.addAllocator("segment-b", second);

    auto result =
        AllocateVChunk(manager, 2U * 4096U, VCSliceSizeLevel::k4K, {}, 2);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(first->size(), 0U);
    EXPECT_EQ(second->size(), 0U);
}

TEST(VChunkAllocationStrategyTest, RejectsInvalidAndEmptyInputs) {
    AllocatorManager manager;
    auto empty = AllocateVChunk(manager, 4096, VCSliceSizeLevel::k4K);
    EXPECT_FALSE(empty.has_value());
    EXPECT_EQ(empty.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    auto zero = AllocateVChunk(manager, 0, VCSliceSizeLevel::k4K);
    EXPECT_FALSE(zero.has_value());
    EXPECT_EQ(zero.error(), ErrorCode::INVALID_PARAMS);

    auto no_replicas =
        AllocateVChunk(manager, 4096, VCSliceSizeLevel::k4K, {}, 0);
    EXPECT_FALSE(no_replicas.has_value());
    EXPECT_EQ(no_replicas.error(), ErrorCode::INVALID_PARAMS);
}

TEST(VChunkAllocationStrategyTest, LimitsSegmentsPerReplica) {
    AllocatorManager manager;
    for (size_t i = 0; i < 4; ++i) {
        auto allocator = std::make_shared<VChunkTestAllocator>(
            "segment-" + std::to_string(i),
            0xB00000000ULL + i * 0x100000, 64U * 1024U);
        manager.addAllocator(allocator->getSegmentName(), allocator);
    }
    VChunkConfig config;
    config.max_segments_per_replica = 2;
    config.max_segments_per_vchunk = 3;
    config.allow_segment_limit_fallback = false;

    auto result = AllocateVChunk(manager, 8U * 4096U,
                                 VCSliceSizeLevel::k4K, {}, 1, config);
    ASSERT_TRUE(result.has_value());
    std::unordered_set<std::string> segments;
    for (const auto& allocation : result->allocations) {
        segments.insert(allocation.segment_name);
    }
    EXPECT_LE(segments.size(), 2U);
}

TEST(VChunkAllocationStrategyTest, FallsBackWhenSegmentLimitIsTooStrict) {
    AllocatorManager manager;
    for (size_t i = 0; i < 2; ++i) {
        auto allocator = std::make_shared<VChunkTestAllocator>(
            "segment-" + std::to_string(i),
            0xC00000000ULL + i * 0x100000, 4096U);
        manager.addAllocator(allocator->getSegmentName(), allocator);
    }
    VChunkConfig config;
    config.max_segments_per_replica = 1;
    config.allow_segment_limit_fallback = true;

    auto result = AllocateVChunk(manager, 2U * 4096U,
                                 VCSliceSizeLevel::k4K, {}, 1, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->allocations.size(), 2U);
}

}  // namespace
}  // namespace mooncake
