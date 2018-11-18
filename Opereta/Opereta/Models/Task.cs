using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace Opereta.Models
{
    public class Task
    {
        public int Id { get; set; }

        public Task()
        {
            this.Participants = new HashSet<TaskEmployee>();
            
        }

        [Required]
        public string Name { get; set; }

        public DateTime CreatedOn { get; set; } = DateTime.UtcNow;
        public DateTime PlanedEndTime { get; set; }
        public DateTime ActualEndTime { get; set; }
        public bool IsInProgress { get; set; } = true;
        public bool IsOverdue { get; set; } = false;
        public Priority Priority { get; set; }
        public string ApplicationUserId { get; set; }
        public virtual ApplicationUser Supervisor { get; set; }

        public virtual ICollection<TaskEmployee> Participants { get; set; }
       

    }
}
