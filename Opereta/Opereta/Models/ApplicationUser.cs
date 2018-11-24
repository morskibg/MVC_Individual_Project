using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Identity;

namespace Opereta.Models
{
    public class ApplicationUser : IdentityUser
    {
        public ApplicationUser()
        {
            this.InvolvedTasks = new HashSet<TaskEmployee>();
            this.MeetingsToParticipate = new HashSet<MeetingEmployee>();
        }
       
        public string FirstName { get; set; }

        public string LastName { get; set; }
        public int Age { get; set; }

        public string PersonalNumber { get; set; }

        public Gender Gender { get; set; }

        public Position Position { get; set; }

        public DateTime HireDate { get; set; } = DateTime.UtcNow;
        public DateTime? ReleaseDate { get; set; }

        public int DepartmentId { get; set; }
        public virtual Department Department { get; set; }

        public ICollection<MeetingEmployee> MeetingsToParticipate { get; set; }
        public ICollection<TaskEmployee> InvolvedTasks { get; set; }
    }
}
