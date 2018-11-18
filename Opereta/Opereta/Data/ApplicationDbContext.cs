using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Opereta.Models;

namespace Opereta.Data
{
    public class ApplicationDbContext : IdentityDbContext<ApplicationUser>
    {
        public DbSet<ApplicationUser> Employees { get; set; }
        public DbSet<Task> Tasks { get; set; }
        public DbSet<Meeting> Meetings { get; set; }
        public DbSet<Department> Departments { get; set; }
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
            : base(options)
        {
        }
        protected override void OnModelCreating(ModelBuilder builder)
        {
            base.OnModelCreating(builder);

            builder.Entity<Department>()
                .HasMany(x => x.Members)
                .WithOne(x => x.Department)
                .HasForeignKey(x => x.DepartmentId);

            builder.Entity<MeetingDepartment>().HasKey(x => new {x.DepartmentId, x.MeetingId});
            builder.Entity<MeetingEmployee>().HasKey(x => new {x.EmployeeId, x.MeetingId});
            builder.Entity<TaskEmployee>().HasKey(x => new {x.EmployeeId, x.TaskId});

        }
    }
}
